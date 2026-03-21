"""Tiered raw lines per BIN: first vs second pile. Synced from web sorter."""

from __future__ import annotations

import json
import random
from pathlib import Path

from data_paths import data_dir

LEADS_PATH = data_dir() / "bin_leads.json"

# Fixed second-tier retail (first tier uses catalog price_per_bin, default 0.90)
SECONDHAND_PRICE_USD = 0.35


def _norm_bin(key: str) -> str | None:
    d = "".join(c for c in str(key) if c.isdigit())[:6]
    return d if len(d) == 6 else None


def norm_stock_tier(t: str) -> str:
    s = str(t).strip().lower()
    if s in ("second", "2", "secondhand", "sh"):
        return "second"
    return "first"


def _tier_dict_normalize(obj) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    if not isinstance(obj, dict):
        return out
    for k, v in obj.items():
        nb = _norm_bin(k)
        if not nb:
            continue
        if isinstance(v, list):
            out[nb] = [str(x).strip() for x in v if str(x).strip()]
        elif isinstance(v, str) and v.strip():
            out[nb] = [v.strip()]
    return out


def _parse_file_raw(raw: dict) -> dict[str, dict[str, list[str]]]:
    """v2: {first:{bin:[]}, second:{}}  v1: {bin:[]} -> all first."""
    if not isinstance(raw, dict):
        return {"first": {}, "second": {}}
    if "first" in raw or "second" in raw:
        return {
            "first": _tier_dict_normalize(raw.get("first")),
            "second": _tier_dict_normalize(raw.get("second")),
        }
    return {"first": _tier_dict_normalize(raw), "second": {}}


def load_all_tiers() -> dict[str, dict[str, list[str]]]:
    LEADS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not LEADS_PATH.is_file():
        payload = {"first": {}, "second": {}}
        LEADS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload
    try:
        raw = json.loads(LEADS_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"first": {}, "second": {}}
    data = _parse_file_raw(raw)
    # Persist migration from v1 → v2 once
    if raw and "first" not in raw and "second" not in raw:
        save_all_tiers(data)
    return data


def save_all_tiers(data: dict[str, dict[str, list[str]]]) -> None:
    LEADS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "first": {k: v for k, v in data.get("first", {}).items() if v},
        "second": {k: v for k, v in data.get("second", {}).items() if v},
    }
    LEADS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_leads(tier: str = "first") -> dict[str, list[str]]:
    t = norm_stock_tier(tier)
    return dict(load_all_tiers().get(t, {}))


def clear_bin_leads() -> None:
    save_all_tiers({"first": {}, "second": {}})


def merge_groups_from_web(groups: dict, tier: str = "first") -> dict:
    """
    Merge groups into first or second pile + catalog BIN list.
    tier: 'first' | 'second'
    """
    from catalog_store import merge_bins_to_catalog

    t = norm_stock_tier(tier)
    all_t = load_all_tiers()
    data = dict(all_t.get(t, {}))
    bin_keys: list[str] = []
    lines_added = 0

    for key, raw_lines in groups.items():
        nb = _norm_bin(key)
        if not nb:
            continue
        bin_keys.append(nb)
        if not isinstance(raw_lines, list):
            continue
        if nb not in data:
            data[nb] = []
        seen = set(data[nb])
        for line in raw_lines:
            s = str(line).strip()
            if not s or s in seen:
                continue
            data[nb].append(s)
            seen.add(s)
            lines_added += 1

    all_t[t] = data
    save_all_tiers(all_t)
    merge_bins_to_catalog(bin_keys)
    return {
        "tier": t,
        "bins_touched": len(set(bin_keys)),
        "lines_added": lines_added,
        "total_bins_with_data": len(data),
    }


def get_lines_for_bin(bin6: str, tier: str = "first") -> list[str]:
    nb = _norm_bin(bin6)
    if not nb:
        return []
    return list(load_leads(tier).get(nb, []))


def state_from_line(line: str) -> str:
    parts = line.split("|")
    if len(parts) <= 7:
        return ""
    s = parts[7].strip().strip('"').strip()
    return s.upper() if s else ""


def total_line_count(tier: str | None = None) -> int:
    if tier is None:
        return total_line_count("first") + total_line_count("second")
    return sum(len(v) for v in load_leads(tier).values())


def bin_line_counts(tier: str = "first") -> dict[str, int]:
    return {b: len(lines) for b, lines in load_leads(tier).items()}


def state_breakdown_for_bin(bin6: str, max_states: int = 6, *, tier: str = "first") -> str:
    lines = get_lines_for_bin(bin6, tier)
    hist: dict[str, int] = {}
    for ln in lines:
        st = state_from_line(ln)
        if st:
            hist[st] = hist.get(st, 0) + 1
    if not hist:
        return ""
    parts = sorted(hist.items(), key=lambda x: (-x[1], x[0]))[:max_states]
    return ", ".join(f"{s}×{c}" for s, c in parts)


def states_compact_for_bin(bin6: str, *, tier: str = "first") -> str:
    lines = get_lines_for_bin(bin6, tier)
    uniq: list[str] = []
    seen: set[str] = set()
    for ln in lines:
        st = state_from_line(ln)
        if st and st not in seen:
            seen.add(st)
            uniq.append(st)
    if not uniq:
        return "—"
    n_distinct = len({state_from_line(l) for l in lines if state_from_line(l)})
    if len(uniq) == 1:
        return uniq[0][:10]
    more = "+" if n_distinct > 2 else ""
    a, b = uniq[0][:5], uniq[1][:5]
    return f"{a}|{b}{more}"[:14]


def _remove_one_line(
    data: dict[str, list[str]], bin6: str, line: str
) -> None:
    lines = data.get(bin6, [])
    try:
        idx = lines.index(line)
    except ValueError:
        return
    lines.pop(idx)
    if not lines:
        del data[bin6]
    else:
        data[bin6] = lines


def restore_pairs_triples(pairs: list[tuple[str, str, str]]) -> None:
    """(bin, line, tier)"""
    if not pairs:
        return
    all_t = load_all_tiers()
    for b, line, tier in pairs:
        k = norm_stock_tier(tier)
        nb = _norm_bin(b)
        if not nb:
            continue
        s = str(line).strip()
        if not s:
            continue
        slot = all_t.setdefault(k, {})
        slot.setdefault(nb, []).append(s)
    save_all_tiers(all_t)


def pop_n_random_from_bin(
    bin6: str, n: int, tier: str = "first"
) -> list[tuple[str, str]] | None:
    t = norm_stock_tier(tier)
    nb = _norm_bin(bin6)
    if not nb or n < 1:
        return None
    all_t = load_all_tiers()
    data = dict(all_t.get(t, {}))
    lines = list(data.get(nb, []))
    if len(lines) < n:
        return None
    picks = random.sample(lines, n)
    for line in picks:
        _remove_one_line(data, nb, line)
    all_t[t] = data
    save_all_tiers(all_t)
    return [(nb, line) for line in picks]


def pop_n_random_any(n: int, tier: str = "first") -> list[tuple[str, str]] | None:
    t = norm_stock_tier(tier)
    if n < 1:
        return None
    all_t = load_all_tiers()
    data = dict(all_t.get(t, {}))
    pool: list[tuple[str, str]] = []
    for b, lines in data.items():
        for line in lines:
            pool.append((b, line))
    if len(pool) < n:
        return None
    chosen = random.sample(pool, n)
    for b, line in chosen:
        _remove_one_line(data, b, line)
    all_t[t] = data
    save_all_tiers(all_t)
    return chosen


def format_notebook_text(bin6: str, lines: list[str]) -> str:
    nb = _norm_bin(bin6) or bin6
    n = len(lines)
    header = "══════════════════════════════════════════════════\n"
    header += f"  BIN: {nb}  |  total entries: {n}\n"
    header += "══════════════════════════════════════════════════\n\n"
    body = "\n".join(f"[{i + 1}] {line}" for i, line in enumerate(lines))
    body += "\n\n—— end of group ——"
    return header + body


def format_sendout_tiers_block() -> str:
    """Telegram sendout: catalog BINs with per-tier counts."""
    from catalog_store import load_catalog

    cat = load_catalog()
    first_p = float(cat.get("price_per_bin", 0.90))
    bins: list[str] = cat.get("bins", [])
    cf = bin_line_counts("first")
    cs = bin_line_counts("second")
    lines = [
        "📤 BIN SENDOUT (two piles)",
        f"Firsthand: ${first_p:.2f}/lead · Secondhand: ${SECONDHAND_PRICE_USD:.2f}/lead",
        "",
    ]
    if not bins:
        lines.append("(no BINs in catalog)")
        return "\n".join(lines)

    lines.append("━━ FIRSTHAND ━━")
    for b in bins:
        n = cf.get(b, 0)
        if n:
            lines.append(f"  {b}  ×{n}")
    if not any(cf.get(b, 0) for b in bins):
        lines.append("  (no firsthand lines)")

    lines.append("")
    lines.append("━━ SECONDHAND ━━")
    for b in bins:
        n = cs.get(b, 0)
        if n:
            lines.append(f"  {b}  ×{n}")
    if not any(cs.get(b, 0) for b in bins):
        lines.append("  (no secondhand lines)")
    return "\n".join(lines)


def stock_tiers_api_payload() -> dict:
    from catalog_store import load_catalog

    cat = load_catalog()
    first_p = float(cat.get("price_per_bin", 0.90))
    bins = cat.get("bins", [])
    cf = bin_line_counts("first")
    cs = bin_line_counts("second")

    def chips(counts: dict[str, int]) -> list[dict]:
        out = []
        for b in bins:
            c = counts.get(b, 0)
            if c:
                out.append({"bin": b, "count": c})
        for b in sorted(counts.keys()):
            if b not in bins and counts[b]:
                out.append({"bin": b, "count": counts[b]})
        return out

    return {
        "first": {
            "price": first_p,
            "total_lines": total_line_count("first"),
            "bins": chips(cf),
        },
        "second": {
            "price": SECONDHAND_PRICE_USD,
            "total_lines": total_line_count("second"),
            "bins": chips(cs),
        },
        "catalog_bins": bins,
    }
