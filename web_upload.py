"""
Serves the BIN tool HTML + catalog API + sendout to Telegram.
Local: http://127.0.0.1:8787/ (or UPLOAD_SERVER_PORT). If PORT is set (e.g. Railway),
binds 0.0.0.0 for a public URL; GET /health for load balancers.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from pathlib import Path

import requests
from flask import Flask, jsonify, request, send_from_directory

from bin_leads_store import merge_groups_from_web, stock_tiers_api_payload
from catalog_store import format_sendout_text, load_catalog

logger = logging.getLogger(__name__)


def create_app(html_file: Path) -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024
    parent = html_file.resolve().parent
    fname = html_file.name

    @app.route("/")
    def index():
        if not html_file.is_file():
            return "<p>BIN tool HTML missing.</p>", 404
        return send_from_directory(str(parent), fname)

    @app.get("/api/catalog")
    def api_catalog():
        return jsonify(load_catalog())

    @app.get("/health")
    def health():
        return jsonify(ok=True), 200

    @app.get("/api/stock-tiers")
    def api_stock_tiers():
        try:
            return jsonify(stock_tiers_api_payload())
        except Exception as e:
            logger.exception("stock-tiers")
            return jsonify(error=str(e)[:200]), 500

    @app.post("/api/sync-groups")
    def api_sync_groups():
        body = request.get_json(silent=True) or {}
        groups = body.get("groups")
        if not isinstance(groups, dict):
            return (
                jsonify(
                    ok=False,
                    error='Expected JSON: { "groups": {...}, "tier": "first"|"second" }',
                ),
                400,
            )
        tier = body.get("tier", "first")
        try:
            stats = merge_groups_from_web(groups, tier=tier)
        except Exception as e:
            logger.exception("sync-groups")
            return jsonify(ok=False, error=str(e)[:240]), 500
        return jsonify(ok=True, **stats)

    @app.post("/api/sendout")
    def api_sendout():
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat = os.environ.get("UPLOAD_NOTIFY_CHAT_ID", "").strip()
        if not token or not chat:
            return (
                jsonify(
                    ok=False,
                    error="Set TELEGRAM_BOT_TOKEN and UPLOAD_NOTIFY_CHAT_ID in .env",
                ),
                400,
            )

        text = format_sendout_text()
        data = load_catalog()
        bins = data.get("bins", [])

        try:
            if len(text) <= 3800:
                r = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={
                        "chat_id": chat,
                        "text": text,
                    },
                    timeout=60,
                )
            else:
                fname_doc = "bin_sendout.txt"
                r = requests.post(
                    f"https://api.telegram.org/bot{token}/sendDocument",
                    data={
                        "chat_id": chat,
                        "caption": (
                            "📤 Sendout — firsthand + secondhand piles "
                            f"({len(bins)} catalog BINs)"
                        ),
                    },
                    files={"document": (fname_doc, text.encode("utf-8"))},
                    timeout=120,
                )
            if not r.ok:
                logger.error("Telegram send failed: %s %s", r.status_code, r.text[:400])
                return (
                    jsonify(ok=False, error="Telegram API error", detail=r.text[:200]),
                    502,
                )
        except requests.RequestException as e:
            logger.exception("sendout request")
            return jsonify(ok=False, error=str(e)[:200]), 502

        return jsonify(ok=True, bins=len(bins))

    return app


def _wait_for_listen(port: int, *, timeout: float = 45.0) -> bool:
    """Block until something accepts TCP on 127.0.0.1:port (Flask bound to 0.0.0.0)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.5):
                return True
        except OSError:
            time.sleep(0.15)
    return False


def run_public_http_forever(html_file: Path) -> None:
    """
    Bind $PORT on 0.0.0.0 in the current thread (blocking).
    Use this on Railway so the container's main process holds the HTTP listener.
    """
    raw = os.environ.get("PORT", "").strip()
    if not raw:
        raise RuntimeError("run_public_http_forever requires PORT")
    p = int(raw)
    host = "0.0.0.0"
    if not html_file.is_file():
        logger.warning(
            "HTML tool not found (%s) — serving /health + APIs only",
            html_file,
        )
    app = create_app(html_file)
    from waitress import serve

    logger.info("Waitress binding %s:%s (main thread, Railway)", host, p)
    serve(app, host=host, port=p, threads=4, channel_timeout=120)


def start_upload_server_background(
    html_file: Path,
    *,
    port: int | None = None,
) -> threading.Thread | None:
    env_port = os.environ.get("PORT", "").strip()
    if not html_file.is_file():
        if not env_port:
            logger.warning(
                "HTML tool not found (%s) — web server not started (set PORT to serve /health only)",
                html_file,
            )
            return None
        logger.warning(
            "HTML tool not found (%s) — serving /health + APIs only (Railway / cloud)",
            html_file,
        )
    if port is not None:
        host, p = "127.0.0.1", int(port)
    elif env_port:
        host, p = "0.0.0.0", int(env_port.strip())
    else:
        host = os.environ.get("FLASK_HOST", "127.0.0.1")
        p = int(os.environ.get("UPLOAD_SERVER_PORT", "8787"))
    app = create_app(html_file)

    def run() -> None:
        try:
            if env_port:
                from waitress import serve

                serve(app, host=host, port=p, threads=4, channel_timeout=120)
            else:
                app.run(
                    host=host,
                    port=p,
                    use_reloader=False,
                    debug=False,
                    threaded=True,
                )
        except Exception:
            logger.exception("Web server thread failed")
            raise

    t = threading.Thread(target=run, daemon=True, name="web-tool")
    t.start()
    if env_port and not _wait_for_listen(p, timeout=45.0):
        logger.error(
            "HTTP server did not bind on port %s within timeout — health checks may fail",
            p,
        )
    if host == "0.0.0.0":
        logger.info("BIN tool + sendout: http://0.0.0.0:%s/ (use your public URL)", p)
    else:
        logger.info("BIN tool + sendout: http://%s:%s/", host, p)
    return t
