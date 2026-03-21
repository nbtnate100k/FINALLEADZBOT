# LeadsBot (Telegram)

Python bot with a **LeadsBot-style** home dashboard: **Browse BINs** (state beside price, add-to-cart), **Buy random** (firsthand vs **$0.35** secondhand, bulk 50/100/150/200 + custom qty, confirm), **custom `qty BIN` → cart**, cart checkout (removes sold lines only), balance / top-up with admin verify, web BIN sorter sync, and `/request`.

## Setup

1. Python 3.10+ recommended.
2. Create a virtualenv and install deps:

```powershell
cd LEADBOT
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
```

3. Put your bot token in `.env` (copy from `.env.example`). This file is listed in `.gitignore` — do not commit real tokens.

4. **Start the bot and leave the window open.** If you close it, Telegram stops receiving replies.

**Easiest (Windows): double‑click `START_BOT.cmd`.** It stops old `LEADBOT` Python bots that were still running old code, then starts this folder’s bot.

Or from PowerShell:

```powershell
.\run.ps1
```

You should see `Logged in as @…` and `build=shop-v7` in the window.

### BIN web tool + sendout

With the bot running, open **http://127.0.0.1:8787/** (same PC). Before **START**, choose **Firsthand** ($0.90, from `catalog.json` `price_per_bin`) or **Secondhand** ($0.35). Sync goes to `/api/sync-groups` with `{ "groups", "tier": "first"|"second" }` → **`data/bin_leads.json`** (two piles) + **`data/catalog.json`**. The page shows **two sendout sections** (live via `/api/stock-tiers`). Telegram **Sendout** lists both piles.

- **Telegram purchases:** **Firsthand BINs** vs **Secondhand BINs** (separate browse). Prices **$0.90** / **$0.35**; random bulk draws only from the matching **pile**. **Cart** can mix tiers; checkout removes lines from the correct pile.  
- **`/addbin`** / **`/clearbin`** (admin): **`/clearbin`** also wipes **`bin_leads.json`** (synced raw lines).

Admins are IDs in `UPLOAD_NOTIFY_CHAT_ID` or `ADMIN_TELEGRAM_IDS` (comma-separated) in `.env`.

`file://` pages cannot call the API reliably — use the `8787` URL.

### “Bot doesn’t answer /start”

- The script must be **running** the whole time you test (polling). No running process = no answers.
- Talk to the **same** bot the token belongs to. For this project, that’s **@LEADv2000bot** (name LEADBOTv2000). Opening a different bot chat will not use this code.
- `.env` is loaded from the **same folder as `bot.py`**, so you can start the bot from any directory once `TELEGRAM_BOT_TOKEN` is set in that `.env`.

### `Conflict: terminated by other getUpdates request`

Telegram allows **only one** long-poll per bot token. That usually means:

- Two windows / two PCs running the **same** bot, or
- A host you forgot (Replit, Railway, VPS, old server), or
- You started the bot again before the previous `getUpdates` finished disconnecting.

**This project:** `START_BOT.cmd` kills local `LEADBOT` Python bots, waits **8 seconds**, then starts one copy. **`bot.py` also binds port `37651`** on your PC so you cannot accidentally run two copies in two windows.

If the error continues after that, **something else on the internet** still uses this token — open [@BotFather](https://t.me/BotFather) → your bot → **Revoke** token → paste the new token into `.env`.

### Top Up still shows “Demo bot: contact admin…”

That text **does not exist** in the current `bot.py`. You are running an **older copy** of the script (or an old process is still polling).

1. Stop **every** `python bot.py` / terminal running the bot (Task Manager → end Python if needed).
2. Start again from **this** folder using `run.ps1` or `.\.venv\Scripts\python.exe bot.py`.
3. In Telegram send **`/version`** — you should see **`shop-v7`** and the full path to the `bot.py` that is actually running.
4. Send **`/start`** and use the **new** message’s buttons (old messages still look like the old UI until you tap fresh buttons).

## Security

If a bot token was ever posted in a chat, issue a new token in [@BotFather](https://t.me/BotFather) (Revoke / regenerate) and update `.env`.

## Customizing

- **BIN products:** `data/catalog.json` (`bins` + `price_per_bin`, default **0.90**). New installs seed **8 default BINs** (see `catalog_store.SEED_BINS`). Stock counts come from **`data/bin_leads.json`** (web **START** sync). State for “Buy by state” is parsed from the **8th pipe-separated field** on each line (same as the web chip summary).
- User balances are stored in `data/users.json` when you extend top-up / checkout logic.
- **Balance / Top Up:** set `PAYMENT_BTC_ADDRESS` / `PAYMENT_LTC_ADDRESS` / `PAYMENT_ETH_ADDRESS` in `.env`. After paying, the user taps **Submit — I sent payment**; admins (`ADMIN_TELEGRAM_IDS` + `UPLOAD_NOTIFY_CHAT_ID`) get **Accept** / **Reject**. Accept credits `data/users.json` balance.

## GitHub → Railway (bot online 24/7)

1. **Install [Git](https://git-scm.com/download/win)** and create an empty repo on [GitHub](https://github.com/new) (no README needed).

2. Open **PowerShell** in your **LEADBOT** project folder (the one that contains `bot.py`), then run — this commits everything **except** `.env` and `.venv` (see `.gitignore`):

```powershell
cd path\to\LEADBOT
git init
git add .
git status
git commit -m "LEADBOT for Railway"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git push -u origin main
```

Replace `YOUR_USERNAME/YOUR_REPO` with your real repo URL from GitHub.

3. **[Railway](https://railway.app)** → **New Project** → **Deploy from GitHub** → pick that repo → select the **web** service (or the only service). Root directory should be the repo root (where **`Dockerfile`** lives).

4. **Variables** tab → add **`TELEGRAM_BOT_TOKEN`** (your bot token from @BotFather). Copy any other lines you use from local **`.env`** (`UPLOAD_NOTIFY_CHAT_ID`, `ADMIN_TELEGRAM_IDS`, etc.) into Railway variables — **never** commit `.env` to GitHub.

5. Railway **deploys automatically**; after it goes green, the bot polls Telegram **non-stop** until you stop/delete the project. **Use only one deployment** per bot token (don’t run the same bot on your PC and Railway at once, or you’ll get **Conflict** errors).

Optional: add a **Volume** + **`LEADBOT_DATA_DIR=/data`** so balances and stock survive redeploys (see below).

---

## Deploy on Railway (details)

1. **Service root:** In Railway, set the service root to the folder that contains **`bot.py`**, **`requirements.txt`**, **`Dockerfile`**, and the BIN HTML file. Nixpacks only detects Python if **`requirements.txt`**, **`main.py`**, **`pyproject.toml`**, or **`Pipfile`** is present — if that file was never committed, the build log will list Python sources but still fail. This repo adds **`Dockerfile`** + **`main.py`** + **`nixpacks.toml`**. **`railway.toml`** sets **`builder = "DOCKERFILE"`** so the image builds with Docker (avoids Nixpacks when the deploy root is incomplete). Nixpacks remains available if you switch the builder in Railway.

2. **New project → Deploy from GitHub** (or upload this repo). Railway sets **`PORT`**; the app binds **`0.0.0.0`** on that port using **Waitress** (not Flask’s dev server). **`railway.toml`** points health checks at **`/health`**. You must set **`TELEGRAM_BOT_TOKEN`** in Railway variables — otherwise `bot.py` exits before the HTTP server starts and the health check will never pass.

3. **Variables** (Dashboard → Variables), at minimum:
   - **`TELEGRAM_BOT_TOKEN`** — from [@BotFather](https://t.me/BotFather)
   - **`UPLOAD_NOTIFY_CHAT_ID`** and/or **`ADMIN_TELEGRAM_IDS`** if you use sendout / admin / top-up flows
   - Optional: **`PAYMENT_*_ADDRESS`**, same as local `.env`

4. **Persistence:** Without a volume, `data/` is wiped on redeploy. Add a **Railway Volume**, mount it (e.g. **`/data`**), and set **`LEADBOT_DATA_DIR=/data`**. The bot writes `users.json`, `catalog.json`, `bin_leads.json`, and `pending_topups.json` under that directory.

5. **Public BIN tool URL:** After deploy, open `https://<your-service>.up.railway.app/` (or your custom domain). Use that URL in the browser instead of `127.0.0.1:8787`.

6. **One poller only** — Same token on **Railway + your PC** (`START_BOT.cmd`) causes **Conflict**: the website can stay “Online” while the bot **stops answering** (HTTP runs, Telegram thread dies). Use **either** cloud **or** local, not both. Do not scale Railway to multiple replicas.

7. **Start command:** `python bot.py` (from **`Dockerfile`** **`CMD`**, **`Procfile`**, or **`railway.toml`**). **`runtime.txt`** is for Nixpacks-only builds.
