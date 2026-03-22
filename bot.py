"""
LeadsBot-style Telegram UI (menus + catalog pagination).
Token: set TELEGRAM_BOT_TOKEN in .env (see .env.example).
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import threading
from html import escape
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
import io

from telegram import (
    BotCommand,
    BotCommandScopeChat,
    InputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import Conflict
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from bin_leads_store import (
    SECONDHAND_PRICE_USD,
    bin_line_counts,
    clear_bin_leads,
    format_notebook_text,
    get_lines_for_bin,
    groups_from_raw_paste,
    merge_groups_from_web,
    norm_stock_tier,
    pop_n_random_any,
    pop_n_random_from_bin,
    restore_pairs_triples,
    state_breakdown_for_bin,
    states_compact_for_bin,
    stock_tiers_api_payload,
    total_line_count,
)

PRICE_SECONDHAND = SECONDHAND_PRICE_USD
from catalog_store import add_bin, clear_all_bins, format_sendout_text, load_catalog
from pending_topups import (
    create_pending,
    get_pending,
    set_status,
    user_has_open_pending,
)
from data_paths import data_dir

_ROOT = Path(__file__).resolve().parent
_BOT_FILE = Path(__file__).resolve()
BOT_BUILD = "shop-v11"
# Only one bot process per PC for this project (avoids Telegram getUpdates Conflict).
_INSTANCE_PORT = 37651
_keepalive_sock: socket.socket | None = None
load_dotenv(_ROOT / ".env")

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def _acquire_single_instance() -> None:
    global _keepalive_sock
    if os.environ.get("SKIP_SINGLE_INSTANCE", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        logger.info("Single-instance lock skipped (SKIP_SINGLE_INSTANCE).")
        return
    if os.environ.get("RAILWAY_ENVIRONMENT", "").strip():
        logger.info("Single-instance lock skipped (RAILWAY_ENVIRONMENT).")
        return
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", _INSTANCE_PORT))
    except OSError as e:
        raise SystemExit(
            f"\n*** Another LEADBOT window is already running on this PC, "
            f"or port {_INSTANCE_PORT} is in use.\n"
            "Close every other LEADBOT / python bot window, wait 10s, then run START_BOT.cmd again.\n***\n"
        ) from e
    s.listen(1)
    _keepalive_sock = s
    logger.info("Single-instance lock OK (port %s).", _INSTANCE_PORT)


DATA_DIR = data_dir()
USERS_PATH = DATA_DIR / "users.json"

ITEMS_PER_PAGE = 10
BULK_RANDOM_QTY = (50, 100, 150, 200)


def _catalog_bin_price() -> float:
    return float(load_catalog().get("price_per_bin", 0.9))


def _catalog_bins_live() -> list[str]:
    return list(load_catalog().get("bins", []))


def _load_users() -> dict:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not USERS_PATH.is_file():
        return {}
    try:
        return json.loads(USERS_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_users(users: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    USERS_PATH.write_text(json.dumps(users, indent=2), encoding="utf-8")


def _read_min_topup_usd() -> float:
    raw = (os.environ.get("MIN_TOPUP_USD") or "").strip()
    if raw:
        try:
            v = float(raw.replace("$", "").replace(",", ""))
            if v > 0:
                return round(v, 2)
        except ValueError:
            logger.warning("Invalid MIN_TOPUP_USD=%r — using default 10", raw)
    return 10.0


MIN_TOPUP_USD = _read_min_topup_usd()
logger.info("Minimum top-up: %s USD (MIN_TOPUP_USD env, default 10)", MIN_TOPUP_USD)


def _min_topup_display() -> str:
    m = MIN_TOPUP_USD
    if abs(m - round(m)) < 1e-6:
        return f"${int(round(m))}"
    return f"${m:.2f}"


def _topup_min_button_label() -> str:
    return _min_topup_display()


def get_balance(user_id: int) -> float:
    users = _load_users()
    entry = users.get(str(user_id), {})
    return float(entry.get("balance", 0.0))


def is_vip(user_id: int) -> bool:
    users = _load_users()
    return bool(users.get(str(user_id), {}).get("vip", False))


def set_balance(user_id: int, value: float) -> None:
    users = _load_users()
    uid = str(user_id)
    users[uid] = {**users.get(uid, {}), "balance": round(value, 2)}
    _save_users(users)


def debit_purchase(user_id: int, amount: float) -> bool:
    """Deduct balance and bump total_spent; False if insufficient funds."""
    if amount <= 0:
        return True
    ensure_user(user_id)
    users = _load_users()
    uid = str(user_id)
    entry = {**_USER_DEFAULTS, **users.get(uid, {})}
    bal = float(entry.get("balance", 0.0))
    if bal + 1e-9 < amount:
        return False
    entry["balance"] = round(bal - amount, 2)
    entry["total_spent"] = round(float(entry.get("total_spent", 0.0)) + amount, 2)
    users[uid] = {**users.get(uid, {}), **entry}
    _save_users(users)
    return True


def refund_purchase(user_id: int, amount: float) -> None:
    if amount <= 0:
        return
    ensure_user(user_id)
    users = _load_users()
    uid = str(user_id)
    entry = {**_USER_DEFAULTS, **users.get(uid, {})}
    entry["balance"] = round(float(entry.get("balance", 0.0)) + amount, 2)
    entry["total_spent"] = round(max(0.0, float(entry.get("total_spent", 0.0)) - amount), 2)
    users[uid] = {**users.get(uid, {}), **entry}
    _save_users(users)


_USER_DEFAULTS: dict = {
    "balance": 0.0,
    "cart": [],
    "vip": False,
    "total_deposits": 0.0,
    "total_spent": 0.0,
    "status": "active",
}


def ensure_user(user_id: int) -> None:
    users = _load_users()
    uid = str(user_id)
    if uid not in users:
        users[uid] = {**_USER_DEFAULTS}
        _save_users(users)
        return
    entry = users[uid]
    changed = False
    for key, val in _USER_DEFAULTS.items():
        if key not in entry:
            entry[key] = val
            changed = True
    if changed:
        _save_users(users)


def get_user_stats(user_id: int) -> dict:
    ensure_user(user_id)
    users = _load_users()
    entry = users.get(str(user_id), {})
    return {**_USER_DEFAULTS, **entry}


def _norm_bin_input(s: str) -> str | None:
    d = "".join(c for c in str(s) if c.isdigit())[:6]
    return d if len(d) == 6 else None


def _normalize_cart_entries(raw) -> list[dict]:
    out: list[dict] = []
    for it in raw or []:
        if not isinstance(it, dict) or it.get("kind") != "bin":
            continue
        nb = _norm_bin_input(str(it.get("bin", "")))
        q = int(it.get("qty", 0) or 0)
        tier = norm_stock_tier(str(it.get("tier", "first")))
        if nb and q > 0:
            out.append({"kind": "bin", "bin": nb, "qty": q, "tier": tier})
    return out


def get_cart_entries(user_id: int) -> list[dict]:
    ensure_user(user_id)
    return _normalize_cart_entries(get_user_stats(user_id).get("cart"))


def save_cart_entries(user_id: int, cart: list[dict]) -> None:
    users = _load_users()
    uid = str(user_id)
    ensure_user(user_id)
    users = _load_users()
    entry = {**users.get(uid, {})}
    entry["cart"] = _normalize_cart_entries(cart)
    users[uid] = entry
    _save_users(users)


def _line_price_for_tier(tier: str) -> float:
    return PRICE_SECONDHAND if norm_stock_tier(tier) == "second" else _catalog_bin_price()


def add_to_cart_bin(user_id: int, bin6: str, qty: int, tier: str = "first") -> None:
    nb = _norm_bin_input(bin6)
    if not nb or qty < 1:
        return
    t = norm_stock_tier(tier)
    cart = get_cart_entries(user_id)
    for it in cart:
        if it["bin"] == nb and it.get("tier", "first") == t:
            it["qty"] += qty
            save_cart_entries(user_id, cart)
            return
    cart.append({"kind": "bin", "bin": nb, "qty": qty, "tier": t})
    save_cart_entries(user_id, cart)


def clear_cart_user(user_id: int) -> None:
    save_cart_entries(user_id, [])


def cart_subtotal_usd(user_id: int) -> float:
    tot = 0.0
    for it in get_cart_entries(user_id):
        tot += it["qty"] * _line_price_for_tier(it.get("tier", "first"))
    return round(tot, 2)


def cart_fulfillment_ok(user_id: int) -> tuple[bool, str]:
    cart = get_cart_entries(user_id)
    need: dict[tuple[str, str], int] = {}
    for it in cart:
        t = it.get("tier", "first")
        k = (it["bin"], t)
        need[k] = need.get(k, 0) + it["qty"]
    for (b, t), n in need.items():
        c = bin_line_counts(t).get(b, 0)
        if c < n:
            tag = "2nd" if norm_stock_tier(t) == "second" else "1st"
            return False, f"BIN {b} ({tag}): need {n}, only {c} in stock."
    return True, ""


def run_cart_checkout(user_id: int) -> tuple[list[tuple[str, str]], float] | None:
    cart = get_cart_entries(user_id)
    if not cart:
        return None
    ok, _ = cart_fulfillment_ok(user_id)
    if not ok:
        return None
    total = cart_subtotal_usd(user_id)
    if get_balance(user_id) + 1e-9 < total:
        return None
    rolled: list[tuple[str, str, str]] = []
    pairs_flat: list[tuple[str, str]] = []
    for it in cart:
        t = it.get("tier", "first")
        got = pop_n_random_from_bin(it["bin"], it["qty"], t)
        if not got or len(got) != it["qty"]:
            restore_pairs_triples(rolled)
            return None
        for b, line in got:
            rolled.append((b, line, t))
            pairs_flat.append((b, line))
    if not debit_purchase(user_id, total):
        restore_pairs_triples(rolled)
        return None
    clear_cart_user(user_id)
    return pairs_flat, total


def random_unit_usd(tier: str) -> float:
    return PRICE_SECONDHAND if tier == "second" else _catalog_bin_price()


def profile_screen_text(user_id: int, u) -> str:
    st = get_user_stats(user_id)
    name = escape(u.full_name or "—")
    uname = f"@{escape(u.username)}" if u.username else "—"
    bal = float(st["balance"])
    dep = float(st["total_deposits"])
    spent = float(st["total_spent"])
    status = escape(str(st.get("status", "active")))
    return (
        "👤 <b>Profile</b>\n\n"
        f"Name: {name}\n"
        f"Username: {uname}\n"
        f"Telegram ID: <code>{user_id}</code>\n"
        f"Balance: <code>${bal:.2f}</code>\n"
        f"Total Deposits: <code>${dep:.2f}</code>\n"
        f"Total Spent: <code>${spent:.2f}</code>\n"
        f"Status: {status}"
    )


def account_balance_text(user_id: int) -> str:
    bal = get_balance(user_id)
    vip_line = "⭐ Active" if is_vip(user_id) else "⭐ Not Active"
    return (
        "💰 <b>Account Balance</b>\n\n"
        f"Balance: <code>${bal:.2f}</code>\n"
        f"VIP: {vip_line}"
    )


def account_balance_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💳 Top Up", callback_data="tub"),
                InlineKeyboardButton("📊 VIP Details", callback_data="vip"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="home")],
        ]
    )


def vip_details_text() -> str:
    return (
        "📊 <b>VIP Details</b>\n\n"
        "Status: <b>Standard</b> (upgrade coming soon)\n\n"
        "VIP perks (when active):\n"
        "• Priority support & restocks\n"
        "• Fee discounts on bulk orders\n"
        "• Early access to new bases\n\n"
        "<i>Contact admin to activate VIP.</i>"
    )


def topup_amount_text() -> str:
    return (
        "💰 <b>Top-Up Your Balance</b>\n\n"
        "Select an amount to add to your balance:\n\n"
        f"• Minimum deposit: <b>{_min_topup_display()}</b>\n"
        "• Payment methods: Crypto (BTC, ETH, LTC)\n\n"
        "Your balance will be updated automatically after payment confirmation."
    )


def topup_amount_keyboard(tu_back: str) -> InlineKeyboardMarkup:
    m = MIN_TOPUP_USD
    row1: list[InlineKeyboardButton] = [
        InlineKeyboardButton(_topup_min_button_label(), callback_data="tumin"),
    ]
    for cb, amt, lbl in (
        ("tua100", 100.0, "$100"),
        ("tua200", 200.0, "$200"),
    ):
        if amt + 1e-9 >= m:
            row1.append(InlineKeyboardButton(lbl, callback_data=cb))
    row2: list[InlineKeyboardButton] = []
    for cb, amt, lbl in (
        ("tua500", 500.0, "$500"),
        ("tua1000", 1000.0, "$1,000"),
    ):
        if amt + 1e-9 >= m:
            row2.append(InlineKeyboardButton(lbl, callback_data=cb))
    row2.append(InlineKeyboardButton("Custom", callback_data="tuac"))
    return InlineKeyboardMarkup(
        [row1, row2, [InlineKeyboardButton("⬅️ Back", callback_data=tu_back)]]
    )


def payment_method_text(amount: float) -> str:
    return (
        "💰 <b>SELECT PAYMENT METHOD</b>\n\n"
        f"Invoice Amount: <code>${amount:.2f} USD</code>\n\n"
        "Available Cryptocurrencies: Choose your preferred payment method:"
    )


def payment_method_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("₿ BTC", callback_data="pmbtc"),
                InlineKeyboardButton("Ł LTC", callback_data="pmltc"),
                InlineKeyboardButton("Ξ ETH", callback_data="pmeth"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="tum")],
        ]
    )


def _payment_address(currency: str) -> str:
    key = {
        "btc": "PAYMENT_BTC_ADDRESS",
        "ltc": "PAYMENT_LTC_ADDRESS",
        "eth": "PAYMENT_ETH_ADDRESS",
    }.get(currency, "")
    return (os.environ.get(key) or "").strip()


def payment_invoice_text(currency: str, amount: float) -> str:
    labels = {"btc": "Bitcoin (BTC)", "ltc": "Litecoin (LTC)", "eth": "Ethereum (ETH)"}
    sym = {"btc": "₿", "ltc": "Ł", "eth": "Ξ"}
    addr = _payment_address(currency)
    if not addr:
        addr = "<i>(Set PAYMENT_%s_ADDRESS in .env)</i>" % currency.upper()
    else:
        addr = f"<code>{addr}</code>"
    return (
        f"{sym.get(currency, '💳')} <b>{labels.get(currency, currency.upper())}</b>\n\n"
        f"Invoice: <code>${amount:.2f} USD</code>\n\n"
        "Send crypto to:\n"
        f"{addr}\n\n"
        "After you send, tap <b>Submit — I sent payment</b> below. "
        "An admin will verify on-chain and credit your balance."
    )


def payment_invoice_markup(currency: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📤 Submit — I sent payment",
                    callback_data=f"tpsub:{currency}",
                )
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="tumpm")],
        ]
    )


def format_admin_topup_message(pid: str, rec: dict) -> str:
    uid = int(rec["user_id"])
    raw_un = (rec.get("username") or "").strip()
    un = f"@{escape(raw_un)}" if raw_un else "—"
    fn = escape(rec.get("full_name") or "—")
    amt = float(rec["amount_usd"])
    c = str(rec.get("currency", "")).upper()
    return (
        "🔔 <b>New top-up to verify</b>\n\n"
        f"ID: <code>{escape(pid)}</code>\n"
        f"User ID: <code>{uid}</code>\n"
        f"Name: {fn}\n"
        f"Username: {un}\n\n"
        f"Amount: <b>${amt:.2f} USD</b>\n"
        f"Method: <b>{escape(c)}</b>\n\n"
        "<i>If funds received, tap Accept.</i>"
    )


def main_menu_keyboard(for_user_id: int | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("📊 Purchase Leads", callback_data="pur")],
        [
            InlineKeyboardButton("💰 My Balance", callback_data="bal"),
            InlineKeyboardButton("💳 Top Up", callback_data="top"),
        ],
        [
            InlineKeyboardButton("🛒 My Cart", callback_data="cart"),
            InlineKeyboardButton("👤 My Profile", callback_data="prof"),
        ],
    ]
    if for_user_id is not None and for_user_id in get_admin_ids():
        rows.append([InlineKeyboardButton("🔧 Admin panel", callback_data="adm")])
    return InlineKeyboardMarkup(rows)


def admin_panel_text() -> str:
    return (
        "🔧 <b>Admin panel</b>\n\n"
        "Same jobs as the BIN web tool — no GitHub Pages or local URL setup.\n"
        "• <b>Stock</b> — firsthand + secondhand line counts per BIN\n"
        "• <b>Sync</b> — paste pipe-separated lines or send a <b>.txt</b> file → chosen pile\n"
        "• <b>Sendout</b> — posts the summary to <code>UPLOAD_NOTIFY_CHAT_ID</code>\n"
        "• <b>BIN notebook</b> — download all lines for one BIN from a pile\n\n"
        "Commands: <code>/addbin</code> · <code>/clearbin</code> · <code>/cancel</code>"
    )


def admin_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📊 Stock summary", callback_data="adm_st")],
            [
                InlineKeyboardButton("📥 Sync (paste / file)", callback_data="adm_sy"),
                InlineKeyboardButton("📤 Sendout", callback_data="adm_so"),
            ],
            [InlineKeyboardButton("📓 BIN notebook", callback_data="adm_nb")],
            [InlineKeyboardButton("⬅️ Home", callback_data="home")],
        ]
    )


def _admin_stock_summary_plain() -> str:
    st = stock_tiers_api_payload()
    lines = [
        "📊 STOCK (two piles)",
        f"Firsthand: {st['first']['total_lines']} lines × ${st['first']['price']:.2f}",
        f"Secondhand: {st['second']['total_lines']} lines × ${st['second']['price']:.2f}",
        "",
        "━━ Firsthand BINs ━━",
    ]
    for row in st["first"]["bins"][:45]:
        lines.append(f"  {row['bin']} ×{row['count']}")
    if len(st["first"]["bins"]) > 45:
        lines.append(f"  … +{len(st['first']['bins']) - 45} more")
    lines += ["", "━━ Secondhand BINs ━━"]
    for row in st["second"]["bins"][:45]:
        lines.append(f"  {row['bin']} ×{row['count']}")
    if len(st["second"]["bins"]) > 45:
        lines.append(f"  … +{len(st['second']['bins']) - 45} more")
    return "\n".join(lines)


def _admin_clear_sync_await(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("awaiting_admin_paste", None)
    context.user_data.pop("admin_sync_tier", None)


def _admin_clear_nb_await(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("awaiting_admin_nb_bin", None)
    context.user_data.pop("admin_nb_tier", None)


async def _deliver_sendout_telegram(bot, chat_id: int) -> tuple[bool, str]:
    text = format_sendout_text()
    try:
        if len(text) <= 3800:
            await bot.send_message(chat_id=chat_id, text=text)
        else:
            bio = io.BytesIO(text.encode("utf-8"))
            await bot.send_document(
                chat_id=chat_id,
                document=InputFile(bio, filename="bin_sendout.txt"),
                caption="📤 Sendout — firsthand + secondhand",
            )
        return True, ""
    except Exception as e:
        return False, str(e)[:220]


def purchase_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💎 Firsthand BINs", callback_data="tfp0"),
                InlineKeyboardButton("♻️ Secondhand BINs", callback_data="tsp0"),
            ],
            [InlineKeyboardButton("🎲 Buy random (bulk)", callback_data="bu_rd")],
            [
                InlineKeyboardButton("📝 Custom → cart (1st)", callback_data="cup_f"),
                InlineKeyboardButton("📝 Custom → cart (2nd)", callback_data="cup_s"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="home")],
        ]
    )


def welcome_text(user_id: int) -> str:
    bal = get_balance(user_id)
    return (
        "👋 <b>LeadsBot - #1 Seller For Bank Leads</b> 🤖\n\n"
        "<b>2026 Fresh Databases</b> — fields per lead: <b>email</b>, <b>phone number</b>, "
        "<b>first &amp; last name</b>, and <b>card number</b>.\n\n"
        "Over 2000+ vouches, most consistent &amp; high quality on the market right now! ⚡\n\n"
        f"💰 <b>Your Balance:</b> ${bal:.2f}\n\n"
        "Use the buttons below to get started! 👇"
    )


def purchase_intro_text() -> str:
    return "💎 <b>Purchase leads</b>\n\n"


def _short_button_label(s: str, max_len: int = 64) -> str:
    s = s.strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def catalog_keyboard(
    page: int,
    bins: list[str],
    price: float,
    counts: dict[str, int],
    stock_tier: str,
) -> tuple[str, InlineKeyboardMarkup]:
    t = norm_stock_tier(stock_tier)
    pfx = "tf" if t == "first" else "ts"
    title = "Firsthand" if t == "first" else "Secondhand"
    total = len(bins)
    start = page * ITEMS_PER_PAGE
    chunk = bins[start : start + ITEMS_PER_PAGE]
    in_stock = sum(counts.get(b, 0) for b in bins)
    lines = [
        f"💎 <b>{title} BINs</b>",
        f"💰 <b>${price:.2f}</b>/lead · 📦 <b>{in_stock}</b> line(s) in this pile",
        "",
        "<b>BIN · qty · $ · state</b>",
    ]
    text = "\n".join(lines)

    rows: list[list[InlineKeyboardButton]] = []
    for local_i, bin6 in enumerate(chunk):
        n = counts.get(bin6, 0)
        st = states_compact_for_bin(bin6, tier=t)
        btn_txt = _short_button_label(
            f"{bin6} ·{n}· ${price:.2f} ·{st}"
        )
        rows.append(
            [InlineKeyboardButton(btn_txt, callback_data=f"{pfx}x{bin6}")]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton("⬅️ Prev", callback_data=f"{pfx}p{page - 1}")
        )
    if start + ITEMS_PER_PAGE < total:
        nav_row.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"{pfx}p{page + 1}")
        )
    if nav_row:
        rows.append(nav_row)

    rows.append(
        [
            InlineKeyboardButton("🔍 Search", callback_data=f"{pfx}sr"),
            InlineKeyboardButton("🔄 Refresh", callback_data=f"{pfx}rf"),
        ]
    )
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="pur")])

    return text, InlineKeyboardMarkup(rows)


def _cart_summary_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💳 Checkout", callback_data="ca_ok"),
                InlineKeyboardButton("🗑 Clear", callback_data="ca_cl"),
            ],
            [InlineKeyboardButton("⬅️ Home", callback_data="home")],
        ]
    )


def cart_screen_markup(user_id: int) -> InlineKeyboardMarkup:
    if not get_cart_entries(user_id):
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("💎 Firsthand", callback_data="tfp0"),
                    InlineKeyboardButton("♻️ Secondhand", callback_data="tsp0"),
                ],
                [InlineKeyboardButton("⬅️ Home", callback_data="home")],
            ]
        )
    return _cart_summary_markup()


def format_cart_screen(user_id: int) -> str:
    cart = get_cart_entries(user_id)
    if not cart:
        return "🛒 <b>My Cart</b>\n\nEmpty."
    lines = ["🛒 <b>My Cart</b> (per-line tier)", ""]
    for i, it in enumerate(cart, 1):
        p = _line_price_for_tier(it.get("tier", "first"))
        tag = "2nd" if norm_stock_tier(it.get("tier", "first")) == "second" else "1st"
        sub = round(it["qty"] * p, 2)
        lines.append(
            f"{i}. BIN <code>{it['bin']}</code> ({tag}) × {it['qty']} @ ${p:.2f} = "
            f"<b>${sub:.2f}</b>"
        )
    tot = cart_subtotal_usd(user_id)
    lines.extend(
        [
            "",
            f"<b>Total:</b> ${tot:.2f}",
            f"Balance: <b>${get_balance(user_id):.2f}</b>",
        ]
    )
    return "\n".join(lines)


async def deliver_purchased_bulk(
    query,
    pairs: list[tuple[str, str]],
    total_price: float,
    title: str,
) -> None:
    body = "\n".join(
        f"[{i + 1}] BIN {b} | {line}" for i, (b, line) in enumerate(pairs)
    )
    caption = (
        f"✅ <b>{escape(title)}</b> · <b>${total_price:.2f}</b> · "
        f"{len(pairs)} line(s) — sold lines removed from stock; BINs unchanged."
    )
    bio = io.BytesIO(body.encode("utf-8"))
    await query.message.reply_document(
        document=InputFile(bio, filename="order.txt"),
        caption=caption,
        parse_mode=ParseMode.HTML,
    )


def random_qty_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for n in BULK_RANDOM_QTY:
        row.append(InlineKeyboardButton(str(n), callback_data=f"rdn{n}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ Type amount", callback_data="rdnc")])
    rows.append([InlineKeyboardButton("⬅️ Change tier", callback_data="rd_t")])
    rows.append([InlineKeyboardButton("⬅️ Purchase menu", callback_data="pur")])
    return InlineKeyboardMarkup(rows)


def random_summary_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Confirm purchase", callback_data="rdok")],
            [
                InlineKeyboardButton("✏️ Change qty", callback_data="rdcq"),
                InlineKeyboardButton("⬅️ Menu", callback_data="pur"),
            ],
        ]
    )


async def show_random_tier_pick(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("rd_tier", None)
    context.user_data.pop("rd_qty", None)
    context.user_data.pop("awaiting_random_qty", None)
    bp = _catalog_bin_price()
    await query.edit_message_text(
        "🎲 <b>Buy random leads</b>\n\nChoose tier:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"Firsthand ${bp:.2f}",
                        callback_data="rdf",
                    ),
                    InlineKeyboardButton(
                        f"Secondhand ${PRICE_SECONDHAND:.2f}",
                        callback_data="rds",
                    ),
                ],
                [InlineKeyboardButton("⬅️ Back", callback_data="pur")],
            ]
        ),
    )


async def show_random_qty_pick(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    tier = context.user_data.get("rd_tier", "first")
    unit = random_unit_usd(tier)
    lbl = "Firsthand" if tier == "first" else "Secondhand"
    n = total_line_count(tier)
    await query.edit_message_text(
        f"🎲 <b>Random · {lbl}</b>\n"
        f"<b>${unit:.2f}</b>/lead · in stock: <b>{n}</b>\n\n"
        "Pick bulk size or type a custom number:",
        parse_mode=ParseMode.HTML,
        reply_markup=random_qty_keyboard(),
    )


async def show_random_confirm(
    query, context: ContextTypes.DEFAULT_TYPE, user_id: int
) -> None:
    tier = context.user_data.get("rd_tier", "first")
    qty = int(context.user_data.get("rd_qty") or 0)
    unit = random_unit_usd(tier)
    total = round(qty * unit, 2)
    lbl = "Firsthand" if tier == "first" else "Secondhand"
    bal = get_balance(user_id)
    await query.edit_message_text(
        f"🎲 <b>Confirm random purchase</b>\n\n"
        f"Tier: <b>{lbl}</b> @ <b>${unit:.2f}</b>/lead\n"
        f"Quantity: <b>{qty}</b>\n"
        f"<b>Total: ${total:.2f}</b>\n"
        f"Your balance: <b>${bal:.2f}</b>\n\n"
        "Tap confirm to charge and receive your file.",
        parse_mode=ParseMode.HTML,
        reply_markup=random_summary_keyboard(),
    )


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user:
        return
    ensure_user(update.effective_user.id)
    uid = update.effective_user.id
    await update.message.reply_text(
        welcome_text(uid),
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(uid),
    )


async def purchase_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """New message with current copy (avoids scrolling to an old edited message)."""
    if not update.effective_user or not update.message:
        return
    ensure_user(update.effective_user.id)
    context.user_data.pop("awaiting_cup", None)
    context.user_data.pop("cup_tier", None)
    context.user_data.pop("await_bin_qty_bin", None)
    context.user_data.pop("await_bin_qty_tier", None)
    context.user_data.pop("awaiting_random_qty", None)
    _admin_clear_sync_await(context)
    _admin_clear_nb_await(context)
    await update.message.reply_text(
        purchase_intro_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=purchase_menu_keyboard(),
    )


async def version_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if update.effective_user.id not in get_admin_ids():
        await update.message.reply_text("⛔ Admin only.")
        return
    try:
        mtime = datetime.fromtimestamp(_BOT_FILE.stat().st_mtime).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except OSError:
        mtime = "?"
    await update.message.reply_text(
        f"🤖 <b>LeadsBot build</b> <code>{BOT_BUILD}</code>\n\n"
        f"<b>Running from</b>\n<code>{_BOT_FILE}</code>\n\n"
        f"<b>File modified</b>: {mtime}\n\n"
        "If menus look outdated: send <code>/start</code> or <code>/purchase</code>, "
        "and stop other copies of this bot (Railway + PC = Conflict). "
        f"Expect short purchase text on <code>{BOT_BUILD}</code>+.",
        parse_mode=ParseMode.HTML,
    )


async def request_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📝 <b>Custom request</b>\n\n"
        "Reply in one message with:\n"
        "<code>TARGET QUANTITY</code>\n\n"
        "Example:\n<code>414720 500</code>\n<code>Jpmorgan Chase 100</code>\n\n"
        "<i>We’ll confirm within 24h (demo bot — no real fulfillment).</i>",
        parse_mode=ParseMode.HTML,
    )


async def show_home(
    query, context: ContextTypes.DEFAULT_TYPE, user_id: int, edit: bool
) -> None:
    text = welcome_text(user_id)
    markup = main_menu_keyboard(user_id)
    if edit:
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=markup
        )
    else:
        await query.message.reply_text(
            text, parse_mode=ParseMode.HTML, reply_markup=markup
        )


def _filter_items(items: list[str], q: str) -> list[str]:
    q = q.strip().lower()
    if not q:
        return list(items)
    return [x for x in items if q in x.lower()]


async def show_catalog_page(
    query, context: ContextTypes.DEFAULT_TYPE, page: int, stock_tier: str
) -> None:
    t = norm_stock_tier(stock_tier)
    context.user_data["browse_tier"] = t
    context.user_data["b1_last_page"] = page
    raw = list(context.user_data.get("CATALOG_BINS", _catalog_bins_live()))
    counts = bin_line_counts(t)
    items = sorted(raw, key=lambda b: (-counts.get(b, 0), b))
    price = _line_price_for_tier(t)
    text, markup = catalog_keyboard(page, items, price, counts, t)
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


FIXED_TOPUP_PRESETS = {
    "tua100": 100.0,
    "tua200": 200.0,
    "tua500": 500.0,
    "tua1000": 1000.0,
}


async def show_topup_menu(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    tu_back: str,
    *,
    as_reply: bool = False,
) -> None:
    """as_reply=True sends a new message (use from main Balance / Top Up so UI is never a stale edit)."""
    context.user_data["tu_back"] = tu_back
    context.user_data.pop("awaiting_topup_custom", None)
    text = topup_amount_text()
    markup = topup_amount_keyboard(tu_back)
    if as_reply:
        await query.message.reply_text(
            text, parse_mode=ParseMode.HTML, reply_markup=markup
        )
    else:
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=markup
        )


async def show_payment_methods(
    query, context: ContextTypes.DEFAULT_TYPE, amount: float
) -> None:
    context.user_data["invoice_usd"] = amount
    await query.edit_message_text(
        payment_method_text(amount),
        parse_mode=ParseMode.HTML,
        reply_markup=payment_method_keyboard(),
    )


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    uid = update.effective_user.id
    if uid in get_admin_ids():
        if context.user_data.get("awaiting_admin_nb_bin"):
            await _admin_consume_notebook_bin(update, context)
            return
        if context.user_data.get("awaiting_admin_paste"):
            await _admin_consume_paste_text(update, context)
            return

    if context.user_data.get("awaiting_topup_custom"):
        raw = (update.message.text or "").strip().replace("$", "").replace(",", "")
        try:
            amt = float(raw)
        except ValueError:
            await update.message.reply_text(
                f"Send a number (min {_min_topup_display()}), e.g. <code>50</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if amt < MIN_TOPUP_USD:
            await update.message.reply_text(
                f"Minimum is <b>{_min_topup_display()}</b>. Try again.",
                parse_mode=ParseMode.HTML,
            )
            return
        context.user_data.pop("awaiting_topup_custom", None)
        context.user_data["invoice_usd"] = amt
        await update.message.reply_text(
            payment_method_text(amt),
            parse_mode=ParseMode.HTML,
            reply_markup=payment_method_keyboard(),
        )
        return

    uid = update.effective_user.id
    ensure_user(uid)

    if context.user_data.get("awaiting_random_qty"):
        raw_t = (update.message.text or "").strip().replace(",", "")
        try:
            rq = int(raw_t)
        except ValueError:
            await update.message.reply_text(
                "Send a whole number, e.g. <code>12</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if rq < 1:
            await update.message.reply_text("Use at least 1.")
            return
        rt = context.user_data.get("rd_tier", "first")
        nstock = total_line_count(rt)
        if rq > nstock:
            await update.message.reply_text(
                f"Max in stock right now: <b>{nstock}</b>.",
                parse_mode=ParseMode.HTML,
            )
            return
        context.user_data.pop("awaiting_random_qty", None)
        context.user_data["rd_qty"] = rq
        tier = context.user_data.get("rd_tier", "first")
        unit = random_unit_usd(tier)
        total = round(rq * unit, 2)
        lbl = "Firsthand" if tier == "first" else "Secondhand"
        bal = get_balance(uid)
        await update.message.reply_text(
            f"🎲 <b>Random · {lbl}</b>\n"
            f"Quantity: <b>{rq}</b> × ${unit:.2f} = <b>${total:.2f}</b>\n"
            f"Balance: <b>${bal:.2f}</b>\n\n"
            "Confirm below.",
            parse_mode=ParseMode.HTML,
            reply_markup=random_summary_keyboard(),
        )
        return

    if context.user_data.get("await_bin_qty_bin"):
        bin6 = str(context.user_data.get("await_bin_qty_bin") or "")
        btier = context.user_data.get("await_bin_qty_tier", "first")
        pfx = "tf" if norm_stock_tier(btier) == "first" else "ts"
        raw_t = (update.message.text or "").strip().replace(",", "")
        try:
            cq = int(raw_t)
        except ValueError:
            await update.message.reply_text(
                "Send a whole number, e.g. <code>12</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if cq < 1:
            await update.message.reply_text("Use at least 1.")
            return
        avail = len(get_lines_for_bin(bin6, btier))
        if cq > avail:
            await update.message.reply_text(
                f"Only <b>{avail}</b> line(s) in this pile for this BIN.",
                parse_mode=ParseMode.HTML,
            )
            return
        context.user_data.pop("await_bin_qty_bin", None)
        context.user_data.pop("await_bin_qty_tier", None)
        add_to_cart_bin(uid, bin6, cq, btier)
        p = _line_price_for_tier(btier)
        await update.message.reply_text(
            "🛒 <b>Added to your cart</b>\n\n"
            f"BIN <code>{escape(bin6)}</code> × <b>{cq}</b> @ "
            f"<b>${p:.2f}</b>/line\n"
            "<i>Item added to your cart.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🛒 Open cart", callback_data="cart")],
                    [
                        InlineKeyboardButton("⬅️ BIN", callback_data=f"{pfx}x{bin6}"),
                    ],
                ]
            ),
        )
        return

    if context.user_data.get("awaiting_cup"):
        text_in = (update.message.text or "").strip()
        m_c = re.match(
            r"^(\d+)\s+(\d{6})\s*$|^(\d{6})\s+(\d+)\s*$",
            text_in,
        )
        if not m_c:
            await update.message.reply_text(
                "Use: <code>10 403491</code> or <code>403491 10</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if m_c.group(1):
            cq, b_raw = int(m_c.group(1)), m_c.group(2)
        else:
            b_raw, cq = m_c.group(3), int(m_c.group(4))
        if b_raw not in set(_catalog_bins_live()):
            await update.message.reply_text(
                "That BIN is not in the catalog. Use Browse BINs.",
                parse_mode=ParseMode.HTML,
            )
            return
        ct = context.user_data.get("cup_tier", "first")
        avail = len(get_lines_for_bin(b_raw, ct))
        if cq > avail:
            await update.message.reply_text(
                f"Only <b>{avail}</b> line(s) in that pile for BIN <code>{escape(b_raw)}</code>.",
                parse_mode=ParseMode.HTML,
            )
            return
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        add_to_cart_bin(uid, b_raw, cq, ct)
        p = _line_price_for_tier(ct)
        await update.message.reply_text(
            "🛒 <b>Added to your cart</b>\n\n"
            f"BIN <code>{escape(b_raw)}</code> × <b>{cq}</b> @ "
            f"<b>${p:.2f}</b>/line\n"
            "<i>Item added to your cart.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🛒 Open cart", callback_data="cart")]]
            ),
        )
        return

    pending = context.user_data.get("search")
    if not pending:
        return

    stier, items_key = pending
    if items_key != "CATALOG_BINS" or stier not in ("first", "second"):
        context.user_data.pop("search", None)
        return

    q = (update.message.text or "").strip()
    context.user_data.pop("search", None)

    items = list(context.user_data.get("CATALOG_BINS", _catalog_bins_live()))
    filtered = _filter_items(items, q)
    context.user_data["CATALOG_BINS"] = filtered

    stier = norm_stock_tier(stier)
    context.user_data["browse_tier"] = stier
    price = _line_price_for_tier(stier)
    counts = bin_line_counts(stier)
    ordered = sorted(filtered, key=lambda b: (-counts.get(b, 0), b))
    context.user_data["b1_last_page"] = 0
    text, markup = catalog_keyboard(0, ordered, price, counts, stier)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


def get_admin_ids() -> set[int]:
    ids: set[int] = set()
    for key in ("ADMIN_TELEGRAM_IDS", "UPLOAD_NOTIFY_CHAT_ID"):
        raw = os.environ.get(key, "").strip()
        for part in raw.replace(",", " ").split():
            part = part.strip()
            if part.isdigit():
                ids.add(int(part))
    return ids


async def clearbin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if update.effective_user.id not in get_admin_ids():
        await update.message.reply_text("⛔ Admin only.")
        return
    n = len(load_catalog().get("bins", []))
    clear_all_bins()
    clear_bin_leads()
    pp = float(load_catalog().get("price_per_bin", 0.9))
    await update.message.reply_text(
        f"🗑 Cleared <b>{n}</b> catalog BIN(s) and <b>both</b> stock piles (first + second).\n"
        f"Firsthand default price when you re-add: <b>${pp:.2f}</b> (<code>/addbin</code> or web START).",
        parse_mode=ParseMode.HTML,
    )


async def addbin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if update.effective_user.id not in get_admin_ids():
        await update.message.reply_text("⛔ Admin only.")
        return
    args = list(context.args or [])
    if not args:
        await update.message.reply_text(
            "Usage: <code>/addbin 414720</code> (6 digits). You can pass several BINs.",
            parse_mode=ParseMode.HTML,
        )
        return
    added = 0
    for a in args:
        if add_bin(a):
            added += 1
    total = len(load_catalog().get("bins", []))
    pp = float(load_catalog().get("price_per_bin", 0.9))
    await update.message.reply_text(
        f"✅ Added <b>{added}</b> BIN(s). Catalog total: <b>{total}</b> @ "
        f"<b>${pp:.2f}</b> each.",
        parse_mode=ParseMode.HTML,
    )


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    uid = update.effective_user.id
    if uid not in get_admin_ids():
        await update.message.reply_text("⛔ Admin only.")
        return
    await update.message.reply_text(
        admin_panel_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=admin_menu_keyboard(),
    )


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    _admin_clear_sync_await(context)
    _admin_clear_nb_await(context)
    await update.message.reply_text(
        "Cancelled — admin sync / notebook prompts cleared.",
        parse_mode=ParseMode.HTML,
    )


async def _admin_consume_paste_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.text:
        return
    tier = context.user_data.get("admin_sync_tier", "first")
    groups = groups_from_raw_paste(msg.text)
    if not groups:
        await msg.reply_text(
            "No valid lines (need <code>card|field|…</code> with 6+ digit card prefix).",
            parse_mode=ParseMode.HTML,
        )
        return
    stats = merge_groups_from_web(groups, tier=tier)
    _admin_clear_sync_await(context)
    pile = "secondhand" if tier == "second" else "firsthand"
    await msg.reply_text(
        f"✅ Synced → <b>{pile}</b>\n"
        f"BINs touched: <b>{stats.get('bins_touched', 0)}</b>\n"
        f"New lines: <b>+{stats.get('lines_added', 0)}</b>",
        parse_mode=ParseMode.HTML,
    )


async def _admin_consume_paste_doc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    doc = msg.document if msg else None
    if not doc:
        return
    if doc.file_size and doc.file_size > 12_000_000:
        await msg.reply_text("File too large (try under ~12 MB).")
        return
    try:
        tg_file = await context.bot.get_file(doc.file_id)
        buf = io.BytesIO()
        await tg_file.download_to_memory(buf)
        raw = buf.getvalue().decode("utf-8", errors="replace")
    except Exception as e:
        await msg.reply_text(f"Could not read file: {e!s}"[:200])
        return
    tier = context.user_data.get("admin_sync_tier", "first")
    groups = groups_from_raw_paste(raw)
    if not groups:
        await msg.reply_text("No valid card lines in that file.")
        return
    stats = merge_groups_from_web(groups, tier=tier)
    _admin_clear_sync_await(context)
    pile = "secondhand" if tier == "second" else "firsthand"
    await msg.reply_text(
        f"✅ From file → <b>{pile}</b>\n"
        f"BINs touched: <b>{stats.get('bins_touched', 0)}</b>\n"
        f"New lines: <b>+{stats.get('lines_added', 0)}</b>",
        parse_mode=ParseMode.HTML,
    )


async def _admin_consume_notebook_bin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.text:
        return
    raw = "".join(c for c in msg.text.strip() if c.isdigit())[:6]
    if len(raw) != 6:
        await msg.reply_text(
            "Send exactly <b>6 digits</b> (one BIN).", parse_mode=ParseMode.HTML
        )
        return
    tier = context.user_data.get("admin_nb_tier", "first")
    lines = get_lines_for_bin(raw, tier)
    if not lines:
        await msg.reply_text(
            f"No lines for <code>{escape(raw)}</code> in the <b>{tier}</b> pile.",
            parse_mode=ParseMode.HTML,
        )
        _admin_clear_nb_await(context)
        return
    body = format_notebook_text(raw, lines)
    bio = io.BytesIO(body.encode("utf-8"))
    tlab = "1st" if tier == "first" else "2nd"
    await msg.reply_document(
        document=InputFile(bio, filename=f"bin_{raw}_{tlab}.txt"),
        caption=f"BIN <code>{raw}</code> · {tlab} pile · {len(lines)} lines",
        parse_mode=ParseMode.HTML,
    )
    _admin_clear_nb_await(context)


async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user or not update.message.document:
        return
    uid = update.effective_user.id
    if uid not in get_admin_ids():
        return
    if not context.user_data.get("awaiting_admin_paste"):
        return
    await _admin_consume_paste_doc(update, context)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    await query.answer()
    user_id = update.effective_user.id
    ensure_user(user_id)
    data = query.data or ""

    if data == "home":
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        context.user_data.pop("await_bin_qty_bin", None)
        context.user_data.pop("await_bin_qty_tier", None)
        context.user_data.pop("awaiting_random_qty", None)
        _admin_clear_sync_await(context)
        _admin_clear_nb_await(context)
        await show_home(query, context, user_id, edit=True)
        return

    if data == "adm":
        if user_id not in get_admin_ids():
            return
        await query.edit_message_text(
            admin_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu_keyboard(),
        )
        return

    if data == "adm_st":
        if user_id not in get_admin_ids():
            return
        summary = _admin_stock_summary_plain()
        await query.message.reply_text(summary)
        return

    if data == "adm_so":
        if user_id not in get_admin_ids():
            return
        raw_chat = os.environ.get("UPLOAD_NOTIFY_CHAT_ID", "").strip()
        if not raw_chat.isdigit():
            await query.message.reply_text(
                "Set <code>UPLOAD_NOTIFY_CHAT_ID</code> in .env for sendout target.",
                parse_mode=ParseMode.HTML,
            )
            return
        ok, err = await _deliver_sendout_telegram(context.bot, int(raw_chat))
        if ok:
            await query.message.reply_text(
                f"✅ Sendout posted to chat <code>{raw_chat}</code>.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await query.message.reply_text(f"⛔ Sendout failed: {escape(err)}", parse_mode=ParseMode.HTML)
        return

    if data == "adm_sy":
        if user_id not in get_admin_ids():
            return
        bp = _catalog_bin_price()
        await query.edit_message_text(
            "📥 <b>Sync leads</b>\n\n"
            f"Choose which pile (same as the web tool). Firsthand uses catalog price "
            f"<b>${bp:.2f}</b> · Secondhand <b>${PRICE_SECONDHAND:.2f}</b>.\n\n"
            "Next step: paste lines or send a <b>.txt</b> file.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            f"Firsthand (${bp:.2f})",
                            callback_data="adm_syf",
                        ),
                        InlineKeyboardButton(
                            f"Secondhand (${PRICE_SECONDHAND:.2f})",
                            callback_data="adm_sys",
                        ),
                    ],
                    [InlineKeyboardButton("⬅️ Admin", callback_data="adm")],
                ]
            ),
        )
        return

    if data in ("adm_syf", "adm_sys"):
        if user_id not in get_admin_ids():
            return
        tier = "first" if data == "adm_syf" else "second"
        context.user_data["admin_sync_tier"] = tier
        context.user_data["awaiting_admin_paste"] = True
        lbl = "Firsthand" if tier == "first" else "Secondhand"
        await query.edit_message_text(
            f"📥 <b>Sync → {lbl}</b>\n\n"
            "Send <b>one message</b> with pipe-separated lines, "
            "or send a <b>.txt</b> / document.\n"
            "<code>/cancel</code> aborts.\n\n"
            "<i>Same grouping as the web START (first 6 digits before |).</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Cancel sync", callback_data="adm_can")]]
            ),
        )
        return

    if data == "adm_can":
        if user_id not in get_admin_ids():
            return
        _admin_clear_sync_await(context)
        await query.edit_message_text(
            admin_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu_keyboard(),
        )
        return

    if data == "adm_nb":
        if user_id not in get_admin_ids():
            return
        await query.edit_message_text(
            "📓 <b>BIN notebook</b>\n\nExport all raw lines for one BIN from a pile.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("Firsthand pile", callback_data="adm_nbf"),
                        InlineKeyboardButton("Secondhand pile", callback_data="adm_nbs"),
                    ],
                    [InlineKeyboardButton("⬅️ Admin", callback_data="adm")],
                ]
            ),
        )
        return

    if data in ("adm_nbf", "adm_nbs"):
        if user_id not in get_admin_ids():
            return
        context.user_data["admin_nb_tier"] = "first" if data == "adm_nbf" else "second"
        context.user_data["awaiting_admin_nb_bin"] = True
        tlab = "Firsthand" if data == "adm_nbf" else "Secondhand"
        await query.edit_message_text(
            f"📓 <b>Notebook · {tlab}</b>\n\n"
            "Send the <b>6-digit BIN</b> in one message.\n<code>/cancel</code> aborts.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Cancel", callback_data="adm_nbx")]]
            ),
        )
        return

    if data == "adm_nbx":
        if user_id not in get_admin_ids():
            return
        _admin_clear_nb_await(context)
        await query.edit_message_text(
            admin_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu_keyboard(),
        )
        return

    if data == "pur":
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        context.user_data.pop("await_bin_qty_bin", None)
        context.user_data.pop("await_bin_qty_tier", None)
        context.user_data.pop("awaiting_random_qty", None)
        _admin_clear_sync_await(context)
        _admin_clear_nb_await(context)
        await query.edit_message_text(
            purchase_intro_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=purchase_menu_keyboard(),
        )
        return

    if data == "bal":
        await query.edit_message_text(
            account_balance_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=account_balance_keyboard(),
        )
        return

    if data == "vip":
        await query.edit_message_text(
            vip_details_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="bal")]]
            ),
        )
        return

    if data == "top":
        await show_topup_menu(query, context, "home", as_reply=True)
        return

    if data == "tub":
        await show_topup_menu(query, context, "bal", as_reply=True)
        return

    if data == "tum":
        tu_back = context.user_data.get("tu_back", "home")
        await show_topup_menu(query, context, tu_back)
        return

    if data == "tumin":
        await show_payment_methods(query, context, MIN_TOPUP_USD)
        return
    if data in FIXED_TOPUP_PRESETS:
        await show_payment_methods(
            query, context, FIXED_TOPUP_PRESETS[data]
        )
        return

    if data == "tuac":
        context.user_data["awaiting_topup_custom"] = True
        context.user_data.pop("search", None)
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        context.user_data.pop("await_bin_qty_bin", None)
        context.user_data.pop("await_bin_qty_tier", None)
        context.user_data.pop("awaiting_random_qty", None)
        await query.edit_message_text(
            "💰 <b>Custom amount</b>\n\n"
            f"Send the amount in USD (minimum <b>{_min_topup_display()}</b>).\n"
            "Example: <code>75</code> or <code>150.50</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="tum")]]
            ),
        )
        return

    if data in ("pmbtc", "pmltc", "pmeth"):
        cur = {"pmbtc": "btc", "pmltc": "ltc", "pmeth": "eth"}[data]
        amt = float(context.user_data.get("invoice_usd", 0.0))
        context.user_data["invoice_currency"] = cur
        await query.edit_message_text(
            payment_invoice_text(cur, amt),
            parse_mode=ParseMode.HTML,
            reply_markup=payment_invoice_markup(cur),
        )
        return

    if data.startswith("tpsub:"):
        cur = data.split(":", 1)[1] if ":" in data else ""
        if cur not in ("btc", "ltc", "eth"):
            await query.answer("Invalid payment type.", show_alert=True)
            return
        amt = float(context.user_data.get("invoice_usd", 0.0))
        if amt <= 0:
            await query.answer("No amount. Start Top Up again.", show_alert=True)
            return
        u = update.effective_user
        if user_has_open_pending(u.id):
            await query.answer(
                "You already have a pending top-up. Wait for admin.", show_alert=True
            )
            return
        pid = create_pending(u.id, u.username, u.full_name, amt, cur)
        if not pid:
            await query.answer("Could not submit. Try again.", show_alert=True)
            return
        rec = get_pending(pid)
        if not rec:
            await query.answer("Error saving request.", show_alert=True)
            return
        admin_kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Accept top-up", callback_data=f"tp_a:{pid}"
                    ),
                    InlineKeyboardButton("❌ Reject", callback_data=f"tp_r:{pid}"),
                ]
            ]
        )
        admin_txt = format_admin_topup_message(pid, rec)
        for aid in get_admin_ids():
            try:
                await context.bot.send_message(
                    chat_id=aid,
                    text=admin_txt,
                    parse_mode=ParseMode.HTML,
                    reply_markup=admin_kb,
                )
            except Exception as e:
                logger.warning("Could not notify admin %s: %s", aid, e)
        await query.edit_message_text(
            payment_invoice_text(cur, amt)
            + "\n\n✅ <b>Submitted.</b> An admin will verify and credit your balance.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ Payment methods", callback_data="tumpm"
                        )
                    ],
                    [InlineKeyboardButton("🏠 Home", callback_data="home")],
                ]
            ),
        )
        await query.answer("Submitted for review.")
        return

    if data.startswith("tp_a:"):
        pid = data.split(":", 1)[1].strip()
        if update.effective_user.id not in get_admin_ids():
            await query.answer("Admin only.", show_alert=True)
            return
        rec = get_pending(pid)
        if not rec or rec.get("status") != "pending":
            await query.answer("Already handled or invalid.", show_alert=True)
            return
        uid = int(rec["user_id"])
        amt = float(rec["amount_usd"])
        ensure_user(uid)
        new_bal = round(get_balance(uid) + amt, 2)
        set_balance(uid, new_bal)
        set_status(pid, "accepted")
        await query.edit_message_text(
            format_admin_topup_message(pid, rec) + "\n\n✅ <b>ACCEPTED</b> — balance credited.",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
        await query.answer("Credited.")
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=(
                    f"✅ Top-up approved: <b>${amt:.2f}</b> added to your balance.\n"
                    f"New balance: <b>${new_bal:.2f}</b>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning("Could not DM user %s: %s", uid, e)
        return

    if data.startswith("tp_r:"):
        pid = data.split(":", 1)[1].strip()
        if update.effective_user.id not in get_admin_ids():
            await query.answer("Admin only.", show_alert=True)
            return
        rec = get_pending(pid)
        if not rec or rec.get("status") != "pending":
            await query.answer("Already handled or invalid.", show_alert=True)
            return
        uid = int(rec["user_id"])
        set_status(pid, "rejected")
        await query.edit_message_text(
            format_admin_topup_message(pid, rec) + "\n\n❌ <b>REJECTED</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
        await query.answer("Rejected.")
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=(
                    "Your top-up request was <b>not</b> approved. "
                    "If you already paid, contact support with your tx id."
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning("Could not DM user %s: %s", uid, e)
        return

    if data == "tumpm":
        amt = float(context.user_data.get("invoice_usd", 0.0))
        await query.edit_message_text(
            payment_method_text(amt),
            parse_mode=ParseMode.HTML,
            reply_markup=payment_method_keyboard(),
        )
        return

    if data == "cart":
        await query.edit_message_text(
            format_cart_screen(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=cart_screen_markup(user_id),
        )
        return

    if data == "prof":
        u = update.effective_user
        await query.edit_message_text(
            profile_screen_text(u.id, u),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="home")]]
            ),
        )
        return

    if data == "bu_rd":
        await show_random_tier_pick(query, context)
        return

    if data == "rdf":
        context.user_data["rd_tier"] = "first"
        await show_random_qty_pick(query, context)
        return

    if data == "rds":
        context.user_data["rd_tier"] = "second"
        await show_random_qty_pick(query, context)
        return

    if data == "rd_t":
        await show_random_tier_pick(query, context)
        return

    m = re.match(r"^rdn(\d+)$", data)
    if m:
        qty = int(m.group(1))
        rt = context.user_data.get("rd_tier", "first")
        nstock = total_line_count(rt)
        if qty > nstock:
            await query.answer(
                f"Only {nstock} line(s) in stock.",
                show_alert=True,
            )
            return
        context.user_data["rd_qty"] = qty
        await show_random_confirm(query, context, user_id)
        return

    if data == "rdnc":
        context.user_data["awaiting_random_qty"] = True
        await query.edit_message_text(
            "✏️ Send how many random leads you want (whole number).",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="rdcq")]]
            ),
        )
        return

    if data == "rdcq":
        context.user_data.pop("rd_qty", None)
        context.user_data.pop("awaiting_random_qty", None)
        if context.user_data.get("rd_tier") not in ("first", "second"):
            await show_random_tier_pick(query, context)
        else:
            await show_random_qty_pick(query, context)
        return

    if data == "rdok":
        tier = context.user_data.get("rd_tier", "first")
        if tier not in ("first", "second"):
            await query.answer("Pick tier again.", show_alert=True)
            return
        qty = int(context.user_data.get("rd_qty") or 0)
        if qty < 1:
            await query.answer("Quantity missing.", show_alert=True)
            return
        unit = random_unit_usd(tier)
        total = round(qty * unit, 2)
        if total_line_count(tier) < qty:
            await query.answer("Not enough stock in that pile.", show_alert=True)
            return
        if get_balance(user_id) + 1e-9 < total:
            await query.answer(
                f"Insufficient balance. Need ${total:.2f}.",
                show_alert=True,
            )
            return
        pairs = pop_n_random_any(qty, tier)
        if not pairs or len(pairs) != qty:
            await query.answer("Could not fulfill. Try again.", show_alert=True)
            return
        if not debit_purchase(user_id, total):
            restore_pairs_triples([(b, line, tier) for b, line in pairs])
            await query.answer("Balance update failed.", show_alert=True)
            return
        context.user_data.pop("rd_tier", None)
        context.user_data.pop("rd_qty", None)
        ttl = "Random firsthand" if tier == "first" else "Random secondhand"
        await deliver_purchased_bulk(query, pairs, total, ttl)
        await query.edit_message_text(
            f"✅ <b>Random order complete</b> · <b>${total:.2f}</b> · {qty} line(s).\n"
            "Your file is in the next message.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Home", callback_data="home")]]
            ),
        )
        await query.answer("Done.")
        return

    if data in ("cup_f", "cup_s"):
        context.user_data["awaiting_cup"] = True
        context.user_data["cup_tier"] = "first" if data == "cup_f" else "second"
        context.user_data.pop("search", None)
        pile = "firsthand" if data == "cup_f" else "secondhand"
        await query.edit_message_text(
            f"📝 <b>Custom → cart ({pile})</b>\n\n"
            "Reply with <code>10 403491</code> or <code>403491 10</code>.\n"
            "We add to cart from that pile if stock allows.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Cancel", callback_data="cup_x")]]
            ),
        )
        return

    if data == "cup_x":
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        await query.edit_message_text(
            purchase_intro_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=purchase_menu_keyboard(),
        )
        return

    if data == "ca_ok":
        if not get_cart_entries(user_id):
            await query.answer("Your cart is empty.", show_alert=True)
            return
        ok, msg = cart_fulfillment_ok(user_id)
        if not ok:
            await query.answer(msg[:180], show_alert=True)
            return
        tot = cart_subtotal_usd(user_id)
        if get_balance(user_id) + 1e-9 < tot:
            await query.answer(
                f"Insufficient balance. Need ${tot:.2f}.",
                show_alert=True,
            )
            return
        res = run_cart_checkout(user_id)
        if not res:
            await query.answer(
                "Checkout failed — stock or balance changed.",
                show_alert=True,
            )
            return
        pairs, total = res
        await deliver_purchased_bulk(query, pairs, total, "Cart checkout")
        await query.edit_message_text(
            f"✅ <b>Cart checkout</b> · <b>${total:.2f}</b> · {len(pairs)} line(s).\n"
            "File attached below.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Home", callback_data="home")]]
            ),
        )
        await query.answer("Purchase complete.")
        return

    if data == "ca_cl":
        clear_cart_user(user_id)
        await query.edit_message_text(
            format_cart_screen(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=cart_screen_markup(user_id),
        )
        await query.answer("Cart cleared.")
        return

    m = re.match(r"^t([fs])p(\d+)$", data)
    if m:
        st = "first" if m.group(1) == "f" else "second"
        await show_catalog_page(query, context, int(m.group(2)), st)
        return

    m = re.match(r"^t([fs])x(\d{6})$", data)
    if m:
        tier_f = "first" if m.group(1) == "f" else "second"
        bin6 = m.group(2)
        pfx = "tf" if tier_f == "first" else "ts"
        allowed = set(context.user_data.get("CATALOG_BINS", _catalog_bins_live()))
        if bin6 not in allowed:
            await query.answer("Open Browse BINs again.", show_alert=True)
            return
        price = _line_price_for_tier(tier_f)
        n = len(get_lines_for_bin(bin6, tier_f))
        states = state_breakdown_for_bin(bin6, tier=tier_f)
        bal = get_balance(user_id)
        st_line = (
            f"States: {escape(states)}\n"
            if states
            else "States: <i>not parsed / empty</i>\n"
        )
        lp = int(context.user_data.get("b1_last_page", 0))
        tlabel = "Firsthand" if tier_f == "first" else "Secondhand"
        await query.edit_message_text(
            f"💎 <b>BIN</b> <code>{escape(bin6)}</code> · <b>{tlabel}</b>\n"
            f"In this pile: <b>{n}</b>\n"
            f"{st_line}"
            f"Price: <b>${price:.2f}</b> per lead\n"
            f"Your balance: <b>${bal:.2f}</b>\n\n"
            "<i>Add to cart — lines removed from this pile only; BIN stays in catalog.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("+1", callback_data=f"{pfx}k{bin6}:1"),
                        InlineKeyboardButton("+5", callback_data=f"{pfx}k{bin6}:5"),
                        InlineKeyboardButton("+10", callback_data=f"{pfx}k{bin6}:10"),
                    ],
                    [
                        InlineKeyboardButton("+25", callback_data=f"{pfx}k{bin6}:25"),
                        InlineKeyboardButton("Custom", callback_data=f"{pfx}k{bin6}:0"),
                    ],
                    [
                        InlineKeyboardButton("🛒 Cart", callback_data="cart"),
                        InlineKeyboardButton("⬅️ Catalog", callback_data=f"{pfx}p{lp}"),
                    ],
                    [InlineKeyboardButton("🏠 Home", callback_data="home")],
                ]
            ),
        )
        return

    m = re.match(r"^t([fs])k(\d{6}):(\d+)$", data)
    if m:
        tier_f = "first" if m.group(1) == "f" else "second"
        pfx = "tf" if tier_f == "first" else "ts"
        bin6 = m.group(2)
        q = int(m.group(3))
        allowed = set(context.user_data.get("CATALOG_BINS", _catalog_bins_live()))
        if bin6 not in allowed:
            await query.answer("Open Browse BINs again.", show_alert=True)
            return
        if q == 0:
            context.user_data["await_bin_qty_bin"] = bin6
            context.user_data["await_bin_qty_tier"] = tier_f
            await query.edit_message_text(
                f"✏️ How many lines for BIN <code>{escape(bin6)}</code> "
                f"({'1st' if tier_f == 'first' else '2nd'} pile)?\n"
                "Send a number in chat.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Back to BIN",
                                callback_data=f"{pfx}x{bin6}",
                            )
                        ]
                    ]
                ),
            )
            return
        avail = len(get_lines_for_bin(bin6, tier_f))
        if q > avail:
            await query.answer(
                f"Only {avail} in this pile for this BIN.",
                show_alert=True,
            )
            return
        add_to_cart_bin(user_id, bin6, q, tier_f)
        await query.answer(f"Added {q} line(s) to cart.")
        lp = int(context.user_data.get("b1_last_page", 0))
        p = _line_price_for_tier(tier_f)
        await query.edit_message_text(
            "🛒 <b>Added to your cart</b>\n\n"
            f"BIN <code>{escape(bin6)}</code> ({'1st' if tier_f == 'first' else '2nd'}) × <b>{q}</b> @ "
            f"<b>${p:.2f}</b>/line\n"
            "<i>Item added to your cart.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🛒 Open cart", callback_data="cart")],
                    [
                        InlineKeyboardButton("⬅️ BIN", callback_data=f"{pfx}x{bin6}"),
                        InlineKeyboardButton("📋 Catalog", callback_data=f"{pfx}p{lp}"),
                    ],
                ]
            ),
        )
        return

    m = re.match(r"^t([fs])sr$", data)
    if m:
        st = "first" if m.group(1) == "f" else "second"
        context.user_data["search"] = (st, "CATALOG_BINS")
        context.user_data["CATALOG_BINS"] = list(_catalog_bins_live())
        lbl = "firsthand" if st == "first" else "secondhand"
        await query.message.reply_text(
            f"🔍 <b>Search {lbl} BINs</b>\n\nEnter digits or part of the BIN.",
            parse_mode=ParseMode.HTML,
        )
        return

    m = re.match(r"^t([fs])rf$", data)
    if m:
        st = "first" if m.group(1) == "f" else "second"
        context.user_data.pop("CATALOG_BINS", None)
        await show_catalog_page(query, context, 0, st)
        return

    logger.warning("Unhandled callback: %s", data)


async def _log_started(app: Application) -> None:
    me = await app.bot.get_me()
    logger.info(
        "Logged in as @%s | build=%s | bot.py=%s",
        me.username,
        BOT_BUILD,
        _BOT_FILE,
    )


async def _register_bot_menu(app: Application) -> None:
    """Populates the blue ▶ Menu button in Telegram (setMyCommands)."""
    default_cmds = [
        BotCommand("start", "Home & main buttons"),
        BotCommand("purchase", "Purchase leads menu"),
        BotCommand("request", "Custom lead request"),
        BotCommand("cancel", "Cancel admin / prompts"),
        BotCommand("panel", "Admin: stock, sync, sendout"),
    ]
    admin_cmds = default_cmds + [
        BotCommand("version", "Build info & tips (admin)"),
        BotCommand("admin", "Admin tools (same as panel)"),
        BotCommand("addbin", "Add BIN lines to catalog"),
        BotCommand("clearbin", "Clear BINs & both stock piles"),
    ]
    try:
        await app.bot.set_my_commands(default_cmds)
        for aid in get_admin_ids():
            try:
                await app.bot.set_my_commands(
                    admin_cmds, scope=BotCommandScopeChat(chat_id=aid)
                )
            except Exception as e:
                logger.warning("set_my_commands admin scope %s: %s", aid, e)
        logger.info("Telegram command menu registered (blue Menu button).")
    except Exception as e:
        logger.warning("set_my_commands failed: %s", e)


async def _post_init(app: Application) -> None:
    await _log_started(app)
    await _register_bot_menu(app)


def _run_telegram_polling(token: str) -> None:
    app = (
        Application.builder()
        .token(token)
        .post_init(_post_init)
        .build()
    )
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("purchase", purchase_cmd))
    app.add_handler(CommandHandler("version", version_cmd))
    app.add_handler(CommandHandler("clearbin", clearbin_cmd))
    app.add_handler(CommandHandler("addbin", addbin_cmd))
    app.add_handler(CommandHandler("request", request_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("panel", admin_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, on_text)
    )

    async def _errors(_update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        err = context.error
        if isinstance(err, Conflict):
            logger.error(
                "Telegram Conflict: this bot TOKEN is being polled from two places at once "
                "(second window, another PC, Replit/Railway, etc.). "
                "Stop every other copy, wait ~30s, then start again — or revoke the token in @BotFather "
                "and put a new one in .env if you do not know what else is running."
            )
            # On Railway, HTTP stays "healthy" while Telegram is dead — exit so the platform restarts
            # and logs show the failure. Only one process may poll this token.
            if os.environ.get("PORT", "").strip():
                os._exit(1)
            return
        logger.error("Handler error", exc_info=err)

    app.add_error_handler(_errors)

    logger.info("Starting poll (Ctrl+C to stop)…")
    # Worker thread (Railway: Telegram beside Waitress on main) cannot use asyncio signal handlers.
    poll_kw: dict = {
        "allowed_updates": Update.ALL_TYPES,
        "drop_pending_updates": False,
    }
    if threading.current_thread() is not threading.main_thread():
        poll_kw["stop_signals"] = None
    app.run_polling(**poll_kw)


def _telegram_thread_main(token: str) -> None:
    """Railway: if this thread crashes, exit the whole container (Waitress alone = 'online' but no replies)."""
    try:
        _run_telegram_polling(token)
    except BaseException:
        logger.exception("Telegram poller exited with an error")
        if os.environ.get("PORT", "").strip():
            os._exit(1)
        raise


def main() -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise SystemExit(
            f"Set TELEGRAM_BOT_TOKEN in {_ROOT / '.env'} "
            "(or export it). Token not found — check you run from any folder: "
            "this bot loads .env next to bot.py."
        )

    _acquire_single_instance()

    _bin_html = _ROOT / "deepseek_html_20260320_822fcc.html"
    env_port = os.environ.get("PORT", "").strip()

    if env_port:
        # Railway expects the container process to listen on $PORT on the main thread.
        try:
            from web_upload import run_public_http_forever

            tg = threading.Thread(
                target=_telegram_thread_main,
                args=(token,),
                name="telegram-bot",
                daemon=False,
            )
            tg.start()
            run_public_http_forever(_bin_html)
        except Exception as e:
            logger.exception("Railway HTTP server failed: %s", e)
            raise
        return

    try:
        from web_upload import start_upload_server_background

        start_upload_server_background(_bin_html)
    except Exception as e:
        logger.warning("Web upload server not started: %s", e)

    _run_telegram_polling(token)


if __name__ == "__main__":
    main()
