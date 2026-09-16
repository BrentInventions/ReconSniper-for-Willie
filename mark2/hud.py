"""Recon Sniper Night Shell — pywebview HUD over the standalone engine."""

from __future__ import annotations

import os
import sys
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FRONTEND = ROOT / "frontend"
PARENT = ROOT.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

from mark2.config import Mark2Config, load_config, SETTINGS_PATH
from mark2.runtime import Mark2Runtime


class _Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(FRONTEND), **kwargs)

    def log_message(self, format, *args):  # noqa: A003
        return


class Mark2Api:
    def __init__(self, runtime: Mark2Runtime) -> None:
        self.rt = runtime

    def snapshot(self) -> dict:
        return self.rt.engine.hud_snapshot()

    def set_mode(self, mode: str) -> str:
        return self.rt.engine.set_mode(str(mode or "LIVE"))

    def set_enabled(self, on: bool) -> bool:
        return self.rt.engine.set_enabled(bool(on))

    def set_contracts(self, n) -> int:
        return self.rt.engine.set_contracts(n)

    def set_daily_goal(self, dollars) -> dict:
        return self.rt.engine.set_daily_goal(dollars)

    def set_goal_enabled(self, on) -> dict:
        return self.rt.engine.set_goal_enabled(bool(on))

    def set_goal_window_hours(self, hours) -> dict:
        return self.rt.engine.set_goal_window_hours(hours)

    def set_stop_points(self, pts) -> float:
        return self.rt.engine.set_stop_points(pts)

    def set_entry_strictness(self, value) -> dict:
        try:
            v = float(value)
        except (TypeError, ValueError):
            v = 50.0
        return self.rt.engine.set_entry_strictness(v)

    def set_entry_gate(self, name: str, value) -> dict:
        try:
            v = float(value)
        except (TypeError, ValueError):
            v = 0.0
        return self.rt.engine.set_entry_gate(str(name or ""), v)

    def set_entry_toggles(self, toggles: dict) -> dict:
        return self.rt.engine.set_entry_toggles(dict(toggles or {}))

    def reset_entry_tuning(self) -> dict:
        return self.rt.engine.reset_entry_tuning()

    def flatten(self) -> str:
        self.rt.engine.flatten_now("HUD_FLAT")
        return "FLAT"

    def exit_trade(self) -> str:
        return self.flatten()

    def buy(self) -> dict:
        return self.rt.engine.manual_order("BUY")

    def sell(self) -> dict:
        return self.rt.engine.manual_order("SELL")

    def clear_session(self) -> dict:
        return self.rt.engine.clear_session()

    def reset_pnl(self) -> dict:
        return self.clear_session()


def _serve() -> tuple[str, ThreadingHTTPServer]:
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{port}/index.html", httpd


def run_hud(
    *,
    headless: bool = False,
    cfg: Mark2Config | None = None,
    persist_path=None,
    title: str | None = None,
    on_ready=None,
) -> int:
    cfg = cfg or load_config(persist_path or SETTINGS_PATH)
    rt = Mark2Runtime(cfg, persist_path=persist_path or SETTINGS_PATH)
    rt.start()
    window_title = str(title or cfg.PRODUCT_NAME or "RECON SNIPER")
    if headless:
        print(
            f"{window_title} headless · mode={cfg.mode().value} · port={cfg.BRIDGE_PORT}",
            flush=True,
        )
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            rt.stop()
        return 0

    url, httpd = _serve()
    try:
        import webview
    except Exception as exc:
        print(f"pywebview missing ({exc}). HUD at {url}", flush=True)
        print("Install: py -3 -m pip install pywebview", flush=True)
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            pass
        rt.stop()
        httpd.shutdown()
        return 1

    api = Mark2Api(rt)
    os.environ.setdefault("WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS", "--autoplay-policy=no-user-gesture-required")
    window = webview.create_window(
        window_title,
        url,
        js_api=api,
        width=1680,
        height=980,
        background_color="#000000",
        text_select=False,
        confirm_close=False,
    )
    gui = "edgechromium" if sys.platform == "win32" else None

    def _max() -> None:
        try:
            window.maximize()
        except Exception:
            pass
        if on_ready:
            try:
                on_ready()
            except Exception:
                pass

    print(f"{window_title} · {url} · engine port {cfg.BRIDGE_PORT}", flush=True)
    try:
        try:
            webview.start(func=_max, gui=gui, debug=False, http_server=False)
        except TypeError:
            webview.start(func=_max, gui=gui, debug=False)
    finally:
        rt.stop()
        try:
            httpd.shutdown()
        except Exception:
            pass
    return 0


def main() -> int:
    headless = "--headless" in sys.argv or os.environ.get("MARK2_NO_WINDOW") == "1"
    return run_hud(headless=headless)


if __name__ == "__main__":
    raise SystemExit(main())
