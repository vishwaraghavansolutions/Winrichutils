"""
scripts/ao_daily_runner.py
──────────────────────────
One-click daily runner — downloads data from Unify, Vested, IPRU email,
and Angel One NXT, then uploads everything to GCS.

Launch via run_daily.bat (double-click on desktop).

Flow
────
  1. GUI window opens showing status.
  2. Edge browser launches → user logs in to Unify.
  3. Phase 1 — Download Unify data.
  4. Phase 2 — Download Vested data (same browser session).
  5. Phase 3 — Download IPRU email attachments via MS Graph API (no browser).
  6. Phase 4 — Login to Angel One NXT → download reports → extract balances.
  7. Shows "All done!" when complete. Edge stays open.

Individual phases can be skipped using the checkboxes in the GUI.
If Edge is closed mid-run, the script detects the disconnect and
prompts the user to log in again — no need to restart.
"""

import ctypes
import ctypes.wintypes
import os
import re
import subprocess
import sys
import threading
import time
import traceback
from datetime import date, timedelta
from pathlib import Path

import tkinter as tk
from tkinter import scrolledtext, font as tkfont

# ── resolve project root so imports work from anywhere ────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Verify the OutlookInboxAgent is importable at startup so failures are caught early
try:
    from agents.outlook_inbox_agent import OutlookInboxAgent as _OIA_CHECK  # noqa: F401
    del _OIA_CHECK
    _OUTLOOK_AGENT_OK = True
except ImportError as _e:
    _OUTLOOK_AGENT_OK = False
    print(f"[warn] OutlookInboxAgent not importable: {_e}  (ICICI PMS phase will be skipped)")

# ── load .env (credentials folder, relative to script) ────────────────────────
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent.parent / "credentials" / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=True)
        print(f"[info] Loaded .env from: {_env_path}")
    else:
        print(
            f"[warn] .env not found at: {_env_path}\n"
            f"       Create credentials/.env with your credentials to proceed."
        )
except ImportError:
    print("[warn] python-dotenv not installed — credentials must be set as environment variables.")

# ── Config ────────────────────────────────────────────────────────────────────
def _find_edge() -> str:
    candidates = [
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    ]
    for path in candidates:
        if Path(path).exists():
            return path
    # Fall back to registry
    try:
        import winreg
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\msedge.exe")
        return winreg.QueryValue(key, None)
    except Exception:
        pass
    raise RuntimeError(
        "Microsoft Edge not found. Please install Edge and try again.\n"
        "Download: https://www.microsoft.com/edge"
    )

EDGE_EXE = _find_edge()
PROFILE_DIR = str(Path.home() / ".angelone" / "edge_profile")
CDP_PORT    = 9222
CDP_URL     = f"http://localhost:{CDP_PORT}"

URL_AUTH           = "https://nxt.angelone.in/auth/authorised-partner"
ANGELONE_OTP_SENDER = "donotreply@angelone.in"
URL_DASHBOARD = "https://nxt.angelone.in/dashboard"
URL_DOWNLOADS = "https://nxt.angelone.in/downloads"

REPORTS = [
    {
        "report_name": "Client DP Holdings",
        "csv_name":    "client-dp-holdings.csv",
        "gcs_bucket":  "winrich",
        "gcs_filename": "client-dp-holdings.csv",
    },
    {
        "report_name": "Security Holdings",
        "csv_name":    "equity.csv",
        "gcs_bucket":  "winrich",
        "gcs_filename": "Equity.csv",
    },
]

IN_CSV  = ROOT / "data" / "Customerlist_12.csv"
OUT_CSV = ROOT / "data" / "equity_margins.csv"
OUT_DIR = ROOT / "data"

# ── Unify config ──────────────────────────────────────────────────────────────
URL_UNIFY_LOGIN    = "https://app.unificap.com/wealthspectrum/portal/sign-in"
URL_UNIFY_AUM      = "https://app.unificap.com/wealthspectrum/portal/bo-queries/Clientwise_Daily_AUM"
URL_UNIFY_DOWNLOAD = URL_UNIFY_AUM
UNIFY_OUT_DIR      = ROOT / "data" / "unify"

# ── ASK config ────────────────────────────────────────────────────────────────
URL_ASK_LOGIN    = "https://askpms.in/wealthspectrum/portal/sign-in"
URL_ASK_AUM      = "https://askpms.in/wealthspectrum/portal/bo-queries/Clientwise_Daily_AUM"
ASK_OUT_DIR      = ROOT / "data" / "ask"
ASK_OTP_SENDER   = "pmsops@askpms.in"

# ── Vested config ─────────────────────────────────────────────────────────────
# TODO: replace with actual Vested login + download URLs
URL_VESTED_LOGIN    = "https://metabase-partners.vestedfinance.com/auth/login/password"
URL_VESTED_DOWNLOAD = ""   # set once the download page is identified
VESTED_OUT_DIR      = ROOT / "data" / "vested"

# ── IPRU email config ─────────────────────────────────────────────────────────
IPRU_SENDER       = "IPRUAUTOMAILER@icicipruamc.com"
IPRU_DOWNLOAD_DIR = ROOT / "data" / "ipru"
IPRU_LOOKBACK_DAYS = 1   # download attachments from emails received in last N days

# ── Email config ───────────────────────────────────────────────────────────────
#EMAIL_FROM    = "winrichgroup@gmail.com"
EMAIL_FROM    = "venkatrag@gmail.com"
EMAIL_TO      = "niranjan@winrich.in"
#EMAIL_APP_PWD = "wbogcehlsoecicaa"
EMAIL_APP_PWD = "angh tkqd amol ruke"   # add spaces to prevent accidental copy-paste

# Set to None to run all customers; set to 10 for testing
TEST_LIMIT    = None  # set to an int (e.g. 10) for testing; None = all customers
TEST_CUSTOMER = None  # set to a username to test one customer; None = all
#TEST_CUSTOMER = "trupti harsh joshi"  # set to a username to test one customer; None = all


# ══════════════════════════════════════════════════════════════════════════════
# GUI
# ══════════════════════════════════════════════════════════════════════════════

class RunnerGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("Daily Runner — Unify · Vested · IPRU Email · Angel One NXT")
        root.geometry("1000x780")
        root.resizable(True, True)
        root.configure(bg="#1e1e2e")

        self._paused    = False
        self._pause_evt = threading.Event()
        self._pause_evt.set()

        _seg  = lambda sz, bold=False: tkfont.Font(family="Segoe UI", size=sz,
                                                    weight="bold" if bold else "normal")
        _mono = lambda sz: tkfont.Font(family="Consolas", size=sz)

        # ── Launch timestamp (audit) ────────────────────────────────────────
        _launched = time.strftime("%d-%b-%Y  %H:%M:%S")
        tk.Label(
            root, text=f"Launched: {_launched}",
            font=_seg(8), fg="#6c7086", bg="#1e1e2e", pady=2,
        ).pack(fill="x")

        # ── Status banner ──────────────────────────────────────────────────
        self.status_var = tk.StringVar(value="Starting…")
        tk.Label(
            root, textvariable=self.status_var,
            font=_seg(13, bold=True), fg="#cdd6f4", bg="#1e1e2e", pady=10,
        ).pack(fill="x")

        # ── Progress bar ───────────────────────────────────────────────────
        self.progress_canvas = tk.Canvas(root, height=6, bg="#313244", highlightthickness=0)
        self.progress_canvas.pack(fill="x", padx=20)
        self._bar         = self.progress_canvas.create_rectangle(0, 0, 0, 6, fill="#89b4fa", outline="")
        self._bar_pos     = 0
        self._bar_running = False

        # ── Buttons row ────────────────────────────────────────────────────
        btn_frame = tk.Frame(root, bg="#1e1e2e")
        btn_frame.pack(pady=(8, 0))

        self.pause_btn = tk.Button(
            btn_frame, text="⏸  Pause", font=_seg(10),
            fg="#1e1e2e", bg="#89b4fa", activebackground="#74c7ec",
            relief="flat", padx=14, pady=5, command=self._toggle_pause,
        )
        self.pause_btn.pack(side="left", padx=(0, 8))
        tk.Button(
            btn_frame, text="✉  Send Email", font=_seg(10),
            fg="#1e1e2e", bg="#a6e3a1", activebackground="#94d89a",
            relief="flat", padx=14, pady=5, command=self._send_email_now,
        ).pack(side="left", padx=(0, 8))
        tk.Button(
            btn_frame, text="✕  Exit", font=_seg(10),
            fg="#1e1e2e", bg="#f38ba8", activebackground="#eba0ac",
            relief="flat", padx=14, pady=5, command=self._on_close,
        ).pack(side="left")

        # ── Options + Phase skips — two-column grid ────────────────────────
        grid_outer = tk.Frame(root, bg="#1e1e2e")
        grid_outer.pack(pady=(10, 0), padx=20, fill="x")

        # Left column: run options
        left = tk.LabelFrame(
            grid_outer, text="  Run Options  ", bg="#1e1e2e",
            fg="#a6adc8", font=_seg(9), bd=1, relief="groove",
        )
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10), pady=2)
        grid_outer.columnconfigure(0, weight=1)

        def _cb(parent, text, var, fg="#cdd6f4", row=0, col=0):
            tk.Checkbutton(
                parent, text=text, variable=var, font=_seg(9),
                fg=fg, bg="#1e1e2e", activeforeground=fg,
                activebackground="#1e1e2e", selectcolor="#313244",
            ).grid(row=row, column=col, sticky="w", padx=10, pady=3)

        self.download_files_var       = tk.BooleanVar(value=False)
        self.resume_var               = tk.BooleanVar(value=False)
        self.skip_balances_var        = tk.BooleanVar(value=False)
        self.angelone_auto_login_var  = tk.BooleanVar(value=False)

        _cb(left, "Re-download files (skip existing CSV check)", self.download_files_var,      row=0, col=0)
        _cb(left, "Resume from last checkpoint",                 self.resume_var,              row=1, col=0)
        _cb(left, "Skip balance extraction",                     self.skip_balances_var,       fg="#f9e2af", row=2, col=0)
        _cb(left, "Angel One NXT — Auto login (default: manual)", self.angelone_auto_login_var, fg="#89dceb", row=3, col=0)

        # Right column: phase skips
        right = tk.LabelFrame(
            grid_outer, text="  Skip Phases  ", bg="#1e1e2e",
            fg="#a6adc8", font=_seg(9), bd=1, relief="groove",
        )
        right.grid(row=0, column=1, sticky="nsew", padx=(0, 0), pady=2)
        grid_outer.columnconfigure(1, weight=1)

        self.skip_unify_var    = tk.BooleanVar(value=False)
        self.skip_ask_var      = tk.BooleanVar(value=False)
        self.skip_vested_var   = tk.BooleanVar(value=False)
        self.skip_email_var    = tk.BooleanVar(value=False)
        self.skip_angelone_var = tk.BooleanVar(value=False)

        _cb(right, "Phase 1 — Unify",          self.skip_unify_var,    fg="#cba6f7", row=0, col=0)
        _cb(right, "Phase 2 — ASK",             self.skip_ask_var,      fg="#cba6f7", row=1, col=0)
        _cb(right, "Phase 3 — Vested",          self.skip_vested_var,   fg="#cba6f7", row=2, col=0)
        _cb(right, "Phase 4 — IPRU Email",      self.skip_email_var,    fg="#cba6f7", row=3, col=0)
        _cb(right, "Phase 5 — Angel One NXT",   self.skip_angelone_var, fg="#cba6f7", row=4, col=0)

        # ── Log area ───────────────────────────────────────────────────────
        self.log = scrolledtext.ScrolledText(
            root, bg="#181825", fg="#cdd6f4",
            font=_mono(9), relief="flat", state="disabled",
        )
        self.log.pack(fill="both", expand=True, padx=10, pady=(10, 10))

        self.log.tag_config("ok",   foreground="#a6e3a1")
        self.log.tag_config("warn", foreground="#f9e2af")
        self.log.tag_config("err",  foreground="#f38ba8")
        self.log.tag_config("info", foreground="#89dceb")

        root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── Pause / resume ─────────────────────────────────────────────────────

    def _toggle_pause(self):
        self._paused = not self._paused
        if self._paused:
            self._pause_evt.clear()
            self.pause_btn.config(text="▶  Resume", bg="#f9e2af")
            self.set_status("Paused — click Resume to continue…")
            self.log_line("⏸ Paused.", "warn")
        else:
            self._pause_evt.set()
            self.pause_btn.config(text="⏸  Pause", bg="#89b4fa")
            self.log_line("▶ Resumed.", "ok")

    def wait_if_paused(self):
        """Automation thread calls this between customers to honour pause."""
        self._pause_evt.wait()

    # ── Helpers ────────────────────────────────────────────────────────────

    def set_status(self, text: str):
        self.root.after(0, lambda: self.status_var.set(text))

    def log_line(self, text: str, tag: str = ""):
        def _do():
            self.log.configure(state="normal")
            self.log.insert("end", text + "\n", tag)
            self.log.see("end")
            self.log.configure(state="disabled")
        self.root.after(0, _do)

    def start_progress(self):
        self._bar_running = True
        self._animate()

    def stop_progress(self):
        self._bar_running = False
        self.progress_canvas.coords(self._bar, 0, 0, 0, 6)

    def _animate(self):
        if not self._bar_running:
            return
        w = self.progress_canvas.winfo_width()
        self._bar_pos = (self._bar_pos + 4) % (w + 80)
        x0 = self._bar_pos - 80
        x1 = self._bar_pos
        self.progress_canvas.coords(self._bar, x0, 0, x1, 6)
        self.root.after(20, self._animate)

    def _send_email_now(self):
        """Retry sending the email from the saved CSV without re-scraping."""
        import pandas as pd

        def _do():
            if not OUT_CSV.exists():
                self.log_line("✉ No saved data found — run the scraper first.", "err")
                return
            try:
                df = pd.read_csv(OUT_CSV)
                captured = int(df["Margin Available for Trade"].notna().sum()) if "Margin Available for Trade" in df.columns else len(df)
                status = _send_report_email(df, len(df), captured, [])
                self.log_line(f"[email] ✓ {status}", "ok")
            except Exception as exc:
                self.log_line(f"[email] Failed: {exc}", "err")

        threading.Thread(target=_do, daemon=True).start()

    def _on_close(self):
        # Close Edge gracefully (no /F) so it doesn't show "closed unexpectedly" next time
        try:
            subprocess.run(["taskkill", "/IM", "msedge.exe"], capture_output=True)
            time.sleep(2)
        except Exception:
            pass
        self.root.destroy()
        sys.exit(0)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _clean_amount(val) -> float | None:
    """Strip ₹, commas, spaces and return a float. Returns None if unparseable."""
    if val is None:
        return None
    cleaned = str(val).replace("₹", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _edge_running() -> bool:
    import socket
    try:
        with socket.create_connection(("localhost", CDP_PORT), timeout=1):
            return True
    except OSError:
        return False


def _minimize_edge():
    user32    = ctypes.windll.user32
    SW_MINIMIZE = 6

    @ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM)
    def _cb(hwnd, _):
        length = user32.GetWindowTextLengthW(hwnd) + 1
        buf    = ctypes.create_unicode_buffer(length)
        user32.GetWindowTextW(hwnd, buf, length)
        title  = buf.value
        if user32.IsWindowVisible(hwnd) and ("Edge" in title or "Angel" in title):
            user32.ShowWindow(hwnd, SW_MINIMIZE)
        return True

    user32.EnumWindows(_cb, 0)


def _restore_edge():
    user32      = ctypes.windll.user32
    SW_RESTORE  = 9

    @ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM)
    def _cb(hwnd, _):
        length = user32.GetWindowTextLengthW(hwnd) + 1
        buf    = ctypes.create_unicode_buffer(length)
        user32.GetWindowTextW(hwnd, buf, length)
        title  = buf.value
        if user32.IsWindowVisible(hwnd) and ("Edge" in title or "Angel" in title):
            user32.ShowWindow(hwnd, SW_RESTORE)
            user32.SetForegroundWindow(hwnd)
        return True

    user32.EnumWindows(_cb, 0)


# ══════════════════════════════════════════════════════════════════════════════
# Phase helpers — Unify, Vested, IPRU Email
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_unify_otp(gui: RunnerGUI, log_fh, triggered_at: float) -> str | None:
    """
    Poll pmsops@unificap.com for the latest Unify OTP email (up to 90 seconds).
    Only accepts an email received AFTER triggered_at (epoch seconds).
    Extracts OTP directly from the preview field — no full body fetch needed.
    Returns the OTP string, or None if not found within the timeout.
    """
    try:
        from agents.outlook_inbox_agent import OutlookInboxAgent
        from agents.base import AgentStatus
    except ImportError as exc:
        _log(gui, log_fh, f"  [Unify OTP] Cannot import OutlookInboxAgent: {exc}", "err")
        return None

    agent    = OutlookInboxAgent()
    mailbox  = os.environ.get("MS_GRAPH_MAILBOX", "").strip().strip('"')
    otp_sender = "pmsops@unificap.com"
    max_wait = 300   # OTP email can take several minutes to arrive
    interval = 10

    _log(gui, log_fh, f"  [Unify OTP] Waiting for OTP email in {mailbox} from {otp_sender}…", "info")

    from datetime import datetime

    for attempt in range(max_wait // interval):
        time.sleep(interval)
        elapsed = (attempt + 1) * interval
        _log(gui, log_fh, f"  [Unify OTP] Checking inbox (attempt {attempt + 1}, {elapsed}s elapsed)…", "info")

        # Search by date only (no sender filter) so Graph API can apply $orderby
        # and return the most recent emails first. Filter by sender in Python.
        result = agent.run("search_emails", {
            "mailbox":        mailbox,
            "received_after": date.today().isoformat(),
            "top":            50,
        })

        if result.status != AgentStatus.SUCCESS:
            _log(gui, log_fh, f"  [Unify OTP] Search error: {result.error}", "warn")
            continue

        all_msgs = result.output.get("messages", [])
        # Filter to OTP sender and sort newest first
        messages = [m for m in all_msgs if m.get("sender_email", "").lower() == otp_sender.lower()]
        messages.sort(key=lambda m: m.get("received_at", ""), reverse=True)
        _log(gui, log_fh, f"  [Unify OTP] Found {len(messages)} email(s) from {otp_sender}.", "info")

        for msg in messages:
            received_str = msg.get("received_at", "")
            preview      = msg.get("preview", "")
            _log(gui, log_fh, f"  [Unify OTP] Email: received={received_str}  preview={preview[:60]!r}", "info")

            try:
                received_ts = datetime.fromisoformat(
                    received_str.replace("Z", "+00:00")
                ).timestamp()
            except ValueError:
                received_ts = 0

            cutoff = triggered_at - 30   # allow 30s clock/delivery buffer
            _log(gui, log_fh, f"  [Unify OTP] received_ts={received_ts:.0f}  cutoff={cutoff:.0f}", "info")

            if received_ts < cutoff:
                _log(gui, log_fh, "  [Unify OTP] Email older than login attempt — skipping.", "info")
                continue

            match = re.search(r'is\s+(\d{4,8})', preview)
            if match:
                otp = match.group(1)
                _log(gui, log_fh, f"  [Unify OTP] ✓ OTP: {otp}", "ok")
                return otp

    _log(gui, log_fh, "  [Unify OTP] OTP not received within timeout.", "err")
    return None


def _gcs_blob_exists(bucket: str, blob_name: str, gui: "RunnerGUI", log_fh) -> bool:
    """Return True if the blob already exists in GCS, False on any error."""
    try:
        from google.cloud import storage as _gcs
        return _gcs.Client().bucket(bucket).blob(blob_name).exists()
    except Exception as exc:
        _log(gui, log_fh, f"  [GCS check] Could not check gs://{bucket}/{blob_name}: {exc}", "warn")
        return False


def _download_unify(page, gui: RunnerGUI, log_fh) -> None:
    """
    Phase 1 — Log in to Unify WealthSpectrum and download BO Queries data.

    Flow:
      1. Navigate to BO Queries page (redirects to login if session expired).
      2. Fill in UNITY_USERNAME and UNITY_PASSWORD from env.
      3. If OTP prompt appears, fetch OTP from pmsops@unificap.com inbox.
      4. Wait for BO Queries page to load.
      5. TODO: trigger export once query/button selectors are confirmed.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    UNIFY_OUT_DIR.mkdir(parents=True, exist_ok=True)

    username = os.environ.get("UNITY_USERNAME", "")
    password = os.environ.get("UNITY_PASSWORD", "")
    if not username or not password:
        _log(gui, log_fh,
             "  [Unify] UNITY_USERNAME or UNITY_PASSWORD not set in .env — skipping.", "err")
        return

    # ── Try AUM page directly — skip login if session is still active ─────────
    _log(gui, log_fh, f"\n► [Unify] Trying AUM page directly…", "info")
    try:
        page.goto(URL_UNIFY_AUM, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass
    except Exception as exc:
        _log(gui, log_fh, f"  [Unify] Navigation warning: {exc}", "warn")

    current_url = page.url
    _log(gui, log_fh, f"  [Unify] Landed on: {current_url}", "info")

    if "sign-in" not in current_url:
        _log(gui, log_fh, "  [Unify] Session active — skipping login.", "ok")
        _unify_run_export(page, gui, log_fh)
        return

    # ── Session expired — go to login page ────────────────────────────────────
    _log(gui, log_fh, "  [Unify] Session expired — logging in…", "info")
    try:
        page.goto(URL_UNIFY_LOGIN, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass
    except Exception as exc:
        _log(gui, log_fh, f"  [Unify] Navigation warning: {exc}", "warn")

    # ── Fill username ─────────────────────────────────────────────────────────
    _log(gui, log_fh, f"  [Unify] Entering credentials for {username}…", "info")
    try:
        user_field = page.locator("input#username").first
        user_field.wait_for(state="visible", timeout=10_000)
        user_field.fill(username)
    except PWTimeout:
        _log(gui, log_fh, "  [Unify] Could not find username field (id='username').", "err")
        return

    # ── Fill password ─────────────────────────────────────────────────────────
    try:
        pwd_field = page.locator("input#password").first
        pwd_field.wait_for(state="visible", timeout=5_000)
        pwd_field.fill(password)
    except PWTimeout:
        _log(gui, log_fh, "  [Unify] Could not find 'Password' field.", "err")
        return

    # ── Submit login form ─────────────────────────────────────────────────────
    _login_triggered_at = time.time()
    page.keyboard.press("Enter")
    _log(gui, log_fh, "  [Unify] Submitted login form.", "info")
    time.sleep(2)

    # ── OTP step ──────────────────────────────────────────────────────────────
    try:
        otp_field = page.locator("input#otp").first
        otp_field.wait_for(state="visible", timeout=8_000)
        _log(gui, log_fh, "  [Unify] OTP prompt detected — fetching from email…", "info")

        otp = _fetch_unify_otp(gui, log_fh, triggered_at=_login_triggered_at)
        if not otp:
            _log(gui, log_fh, "  [Unify] Could not retrieve OTP — login aborted.", "err")
            return

        otp_field.fill(otp)
        page.keyboard.press("Enter")
        _log(gui, log_fh, "  [Unify] OTP submitted.", "ok")
        time.sleep(2)
    except PWTimeout:
        _log(gui, log_fh, "  [Unify] OTP field not found (expected input#otp).", "err")
        return

    # ── Wait for post-login redirect (dashboard or bo-queries) ───────────────
    try:
        page.wait_for_url(
            lambda u: "sign-in" not in u and "wealthspectrum/portal" in u,
            timeout=30_000,
        )
        _log(gui, log_fh, f"  [Unify] Logged in — landed on: {page.url}", "ok")
    except PWTimeout:
        _log(gui, log_fh, f"  [Unify] Login may not have completed. URL: {page.url}", "warn")

    _unify_run_export(page, gui, log_fh)


def _unify_run_export(page, gui: RunnerGUI, log_fh) -> None:
    """
    Navigate to Clientwise Daily AUM query via keyboard tab navigation,
    set From/To date to today, click Execute and save the download.

    Flow (mirrors the Angel One NXT tab-navigation pattern):
      1. Tab from dashboard to "Queries" nav item → Enter
      2. Tab to "Clientwise Daily AUM" in the query list → Enter
      3. Tab to first date input (From Date) → type today
      4. Tab to second date input (To Date) → type today
      5. Tab to Execute → Enter → save download
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    today     = date.today() - timedelta(days=1)   # Unify reports run for previous day
    today_dmy = today.strftime("%d/%m/%Y")  # DD/MM/YYYY for Angular Material datepicker

    _log(gui, log_fh, "\n  [Unify] Navigating to Clientwise Daily AUM via keyboard…", "info")

    # ── Keyboard-navigation helpers ───────────────────────────────────────────

    def _focused():
        """Return info about the currently focused element."""
        return page.evaluate("""() => {
            const el = document.activeElement;
            if (!el) return {tag: '', text: '', placeholder: '', type: ''};
            return {
                tag:         el.tagName,
                text:        (el.innerText || el.textContent || '').trim(),
                placeholder: el.placeholder || '',
                type:        el.type || '',
            };
        }""")

    def _tab_to_text(target: str, max_tabs: int = 60) -> bool:
        """Tab until the focused element's text equals target, then press Enter."""
        for i in range(max_tabs):
            info = _focused()
            if info['text'] == target:
                _log(gui, log_fh, f"  [Unify] Tab #{i}: found '{target}' → Enter", "info")
                page.keyboard.press("Enter")
                return True
            page.keyboard.press("Tab")
            time.sleep(0.15)
        _log(gui, log_fh, f"  [Unify] Could not reach '{target}' in {max_tabs} tabs.", "warn")
        return False

    def _tab_to_date_input(max_tabs: int = 40) -> bool:
        """Tab until focused on an input with placeholder 'Choose a date'."""
        for i in range(max_tabs):
            info = _focused()
            if info['tag'] == 'INPUT' and info['placeholder'] == 'Choose a date':
                _log(gui, log_fh, f"  [Unify] Tab #{i}: on date input", "info")
                return True
            page.keyboard.press("Tab")
            time.sleep(0.15)
        _log(gui, log_fh, f"  [Unify] Could not reach date input in {max_tabs} tabs.", "warn")
        return False

    def _tab_to_execute(max_tabs: int = 20) -> bool:
        """Tab until focused on the Execute button."""
        for i in range(max_tabs):
            info = _focused()
            if info['text'].upper() == 'EXECUTE':
                _log(gui, log_fh, f"  [Unify] Tab #{i}: on Execute button", "info")
                return True
            page.keyboard.press("Tab")
            time.sleep(0.15)
        _log(gui, log_fh, f"  [Unify] Could not reach Execute in {max_tabs} tabs.", "warn")
        return False

    # ── Step 1: focus the page and tab to "Queries" ───────────────────────────
    page.evaluate("() => { document.body.focus(); }")
    time.sleep(0.3)

    if not _tab_to_text("Queries"):
        _log(gui, log_fh, "  [Unify] Aborting — could not reach Queries.", "err")
        return
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass
    time.sleep(1)

    # ── Step 1b: press Tab 6 times from "Queries" focus, then Enter to open query list
    _log(gui, log_fh, "  [Unify] Tabbing 6 times to hamburger button…", "info")
    for _ in range(6):
        page.keyboard.press("Tab")
        time.sleep(0.15)
    page.keyboard.press("Enter")
    time.sleep(1)   # let the query list panel animate open
    _log(gui, log_fh, "  [Unify] Query list opened.", "info")

    # ── Step 2: tab to "Clientwise Daily AUM" and select it ──────────────────
    if not _tab_to_text("Clientwise Daily AUM"):
        _log(gui, log_fh, "  [Unify] Aborting — could not reach Clientwise Daily AUM.", "err")
        return
    time.sleep(0.5)

    # The query list panel stays open after selection. Click the right half of
    # the viewport to dismiss the panel and reveal the query form.
    vp = page.viewport_size or {"width": 1280, "height": 800}
    page.mouse.click(vp["width"] * 3 // 4, vp["height"] // 2)
    time.sleep(1)
    _log(gui, log_fh, "  [Unify] Dismissed query panel — form should be visible.", "info")

    # ── Step 3: From Date ─────────────────────────────────────────────────────
    if _tab_to_date_input():
        page.keyboard.type(today_dmy, delay=50)
        page.keyboard.press("Escape")
        time.sleep(0.3)
        _log(gui, log_fh, f"  [Unify] From Date → {today_dmy}", "info")
        # Tab past the From Date field (and its calendar-icon button) to reach To Date
        page.keyboard.press("Tab")
        time.sleep(0.2)
        page.keyboard.press("Tab")
        time.sleep(0.2)

    # ── Step 4: To Date ───────────────────────────────────────────────────────
    if _tab_to_date_input():
        page.keyboard.type(today_dmy, delay=50)
        page.keyboard.press("Escape")
        time.sleep(0.3)
        _log(gui, log_fh, f"  [Unify] To Date   → {today_dmy}", "info")

    # ── Step 4b: Format selection → CSV, then Execute ────────────────────────
    # After To Date: Tab → format control → Enter (opens list, CSV is first) →
    # Enter (selects CSV) → Tab → Execute → Enter
    _log(gui, log_fh, "  [Unify] Tabbing to format selector…", "info")
    page.keyboard.press("Tab")
    time.sleep(0.3)
    page.keyboard.press("Enter")   # open the format dropdown
    time.sleep(0.4)
    page.keyboard.press("Enter")   # select CSV (first option)
    time.sleep(0.3)
    _log(gui, log_fh, "  [Unify] CSV format selected.", "info")

    # Tab to Execute
    page.keyboard.press("Tab")
    time.sleep(0.2)

    # ── Step 5: Execute ───────────────────────────────────────────────────────
    if not _tab_to_execute():
        _log(gui, log_fh, "  [Unify] Could not reach Execute — aborting.", "err")
        return

    UNIFY_OUT_DIR.mkdir(parents=True, exist_ok=True)
    _log(gui, log_fh, "  [Unify] Pressing Execute…", "info")

    page.keyboard.press("Enter")
    _log(gui, log_fh, "  [Unify] Execute pressed — waiting for results to load…", "info")
    try:
        page.wait_for_load_state("networkidle", timeout=20_000)
    except PWTimeout:
        pass
    time.sleep(2)

    # Wait for results to load — the download button appears as 'sim_card_download'
    _log(gui, log_fh, "  [Unify] Waiting for results and download button…", "info")
    deadline = time.time() + 60
    while time.time() < deadline:
        time.sleep(2)
        if "sim_card_download" in page.content():
            break
    else:
        _log(gui, log_fh, "  [Unify] Results did not appear within 60s.", "err")
        return

    # Page auto-refreshes every 10s and loses focus — tab to sim_card_download
    # and press Enter to trigger the file download.
    _log(gui, log_fh, "  [Unify] Results ready — tabbing to download button…", "info")
    UNIFY_OUT_DIR.mkdir(parents=True, exist_ok=True)
    page.evaluate("() => { document.body.focus(); }")
    time.sleep(0.3)

    try:
        with page.expect_download(timeout=30_000) as dl_info:
            if not _tab_to_text("sim_card_download"):
                _log(gui, log_fh, "  [Unify] Could not find download button.", "err")
                return
            page.keyboard.press("Enter")
            _log(gui, log_fh, "  [Unify] Download triggered — saving file…", "info")

        download  = dl_info.value
        save_path = UNIFY_OUT_DIR / download.suggested_filename
        download.save_as(save_path)
        _log(gui, log_fh, f"  [Unify] ✓ Downloaded: {save_path.name}", "ok")

        # Upload to GCS: winrich / Datawarehouse/Unify/YYYY/MM/DD/WAWYA_Daily_AUM_DD-MM-YYYY.csv
        try:
            sys.path.insert(0, str(ROOT))
            from agents.gcs_storage_agent import GCSStorageAgent
            from agents.base import AgentStatus as _AS
            _t = date.today() - timedelta(days=1)   # report is for previous day
            gcs_filename = f"WAWYA_Daily_AUM_{_t.strftime('%d-%m-%Y')}.csv"
            gcs_prefix   = f"Datawarehouse/Unify/{_t.year}/{_t.month:02d}/{_t.day:02d}"
            gcs = GCSStorageAgent()
            result = gcs.run("upload_csv", {
                "file_path":   str(save_path),
                "filename":    gcs_filename,
                "bucket_name": "winrich",
                "prefix":      gcs_prefix,
            })
            if result.status == _AS.SUCCESS:
                _log(gui, log_fh, f"  [Unify] ✓ Uploaded to GCS: {result.output.get('gcs_uri')}", "ok")
                try:
                    save_path.unlink()
                    _log(gui, log_fh, f"  [Unify] Local file removed: {save_path.name}", "info")
                except Exception as del_exc:
                    _log(gui, log_fh, f"  [Unify] Could not remove local file: {del_exc}", "warn")
            else:
                _log(gui, log_fh, f"  [Unify] GCS upload failed: {result.error}", "err")
        except Exception as exc:
            _log(gui, log_fh, f"  [Unify] GCS upload error: {exc}", "err")

    except PWTimeout:
        _log(gui, log_fh, "  [Unify] Download did not complete within 30s.", "err")




def _download_ask(page, gui: RunnerGUI, log_fh) -> None:
    """
    Phase 2 — Log in to ASK PMS WealthSpectrum and download Clientwise Daily AUM.
    Identical flow to Unify but using askpms.in domain.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    ASK_OUT_DIR.mkdir(parents=True, exist_ok=True)

    username = os.environ.get("ASK_USERNAME", "")
    password = os.environ.get("ASK_PASSCODE", "")
    if not username or not password:
        _log(gui, log_fh,
             "  [ASK] ASK_USERNAME or ASK_PASSCODE not set in .env — skipping.", "err")
        return

    _log(gui, log_fh, f"\n► [ASK] Trying AUM page directly…", "info")
    try:
        page.goto(URL_ASK_AUM, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass
    except Exception as exc:
        _log(gui, log_fh, f"  [ASK] Navigation warning: {exc}", "warn")

    current_url = page.url
    _log(gui, log_fh, f"  [ASK] Landed on: {current_url}", "info")

    if "sign-in" not in current_url:
        _log(gui, log_fh, "  [ASK] Session active — skipping login.", "ok")
        _ask_run_export(page, gui, log_fh)
        return

    _log(gui, log_fh, "  [ASK] Session expired — logging in…", "info")
    try:
        page.goto(URL_ASK_LOGIN, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass
    except Exception as exc:
        _log(gui, log_fh, f"  [ASK] Navigation warning: {exc}", "warn")

    _log(gui, log_fh, f"  [ASK] Entering credentials for {username}…", "info")
    try:
        user_field = page.locator("input#username").first
        user_field.wait_for(state="visible", timeout=10_000)
        user_field.fill(username)
    except PWTimeout:
        _log(gui, log_fh, "  [ASK] Could not find username field (id='username').", "err")
        return

    try:
        pwd_field = page.locator("input#password").first
        pwd_field.wait_for(state="visible", timeout=5_000)
        pwd_field.fill(password)
    except PWTimeout:
        _log(gui, log_fh, "  [ASK] Could not find password field.", "err")
        return

    page.keyboard.press("Enter")
    _log(gui, log_fh, "  [ASK] Submitted login form.", "info")
    time.sleep(2)

    try:
        page.wait_for_url(
            lambda u: "sign-in" not in u and "wealthspectrum/portal" in u,
            timeout=30_000,
        )
        _log(gui, log_fh, f"  [ASK] Logged in — landed on: {page.url}", "ok")
    except PWTimeout:
        _log(gui, log_fh, f"  [ASK] Login may not have completed. URL: {page.url}", "warn")

    _ask_run_export(page, gui, log_fh)


def _ask_run_export(page, gui: RunnerGUI, log_fh) -> None:
    """Navigate to Clientwise Daily AUM on ASK PMS and download as CSV."""
    from playwright.sync_api import TimeoutError as PWTimeout

    today     = date.today() - timedelta(days=1)
    today_dmy = today.strftime("%d/%m/%Y")

    _log(gui, log_fh, "\n  [ASK] Navigating to Clientwise Daily AUM via keyboard…", "info")

    def _focused():
        return page.evaluate("""() => {
            const el = document.activeElement;
            if (!el) return {tag: '', text: '', placeholder: '', type: ''};
            return {
                tag:         el.tagName,
                text:        (el.innerText || el.textContent || '').trim(),
                placeholder: el.placeholder || '',
                type:        el.type || '',
            };
        }""")

    def _tab_to_text(target, max_tabs=60):
        for i in range(max_tabs):
            info = _focused()
            if info['text'] == target:
                _log(gui, log_fh, f"  [ASK] Tab #{i}: found '{target}' → Enter", "info")
                page.keyboard.press("Enter")
                return True
            page.keyboard.press("Tab")
            time.sleep(0.15)
        _log(gui, log_fh, f"  [ASK] Could not reach '{target}' in {max_tabs} tabs.", "warn")
        return False

    def _tab_to_date_input(max_tabs=40):
        for i in range(max_tabs):
            info = _focused()
            if info['tag'] == 'INPUT' and info['placeholder'] == 'Choose a date':
                _log(gui, log_fh, f"  [ASK] Tab #{i}: on date input", "info")
                return True
            page.keyboard.press("Tab")
            time.sleep(0.15)
        _log(gui, log_fh, f"  [ASK] Could not reach date input in {max_tabs} tabs.", "warn")
        return False

    def _tab_to_execute(max_tabs=20):
        for i in range(max_tabs):
            info = _focused()
            if info['text'].upper() == 'EXECUTE':
                _log(gui, log_fh, f"  [ASK] Tab #{i}: on Execute button", "info")
                return True
            page.keyboard.press("Tab")
            time.sleep(0.15)
        _log(gui, log_fh, f"  [ASK] Could not reach Execute in {max_tabs} tabs.", "warn")
        return False

    page.evaluate("() => { document.body.focus(); }")
    time.sleep(0.3)

    if not _tab_to_text("Queries"):
        _log(gui, log_fh, "  [ASK] Aborting — could not reach Queries.", "err")
        return
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass
    time.sleep(1)

    _log(gui, log_fh, "  [ASK] Tabbing 10 times to hamburger button…", "info")
    for _ in range(10):
        page.keyboard.press("Tab")
        time.sleep(0.15)
    page.keyboard.press("Enter")
    time.sleep(1)
    _log(gui, log_fh, "  [ASK] Query list opened.", "info")

    if not _tab_to_text("Clientwise Daily AUM"):
        _log(gui, log_fh, "  [ASK] Aborting — could not reach Clientwise Daily AUM.", "err")
        return
    time.sleep(0.5)

    vp = page.viewport_size or {"width": 1280, "height": 800}
    page.mouse.click(vp["width"] * 3 // 4, vp["height"] // 2)
    time.sleep(1)
    _log(gui, log_fh, "  [ASK] Dismissed query panel — form should be visible.", "info")

    if _tab_to_date_input():
        page.keyboard.type(today_dmy, delay=50)
        page.keyboard.press("Escape")
        time.sleep(0.3)
        _log(gui, log_fh, f"  [ASK] From Date → {today_dmy}", "info")
        page.keyboard.press("Tab")
        time.sleep(0.2)
        page.keyboard.press("Tab")
        time.sleep(0.2)

    if _tab_to_date_input():
        page.keyboard.type(today_dmy, delay=50)
        page.keyboard.press("Escape")
        time.sleep(0.3)
        _log(gui, log_fh, f"  [ASK] To Date   → {today_dmy}", "info")

    _log(gui, log_fh, "  [ASK] Tabbing to format selector…", "info")
    page.keyboard.press("Tab")
    time.sleep(0.3)
    page.keyboard.press("Enter")
    time.sleep(0.4)
    page.keyboard.press("Enter")
    time.sleep(0.3)
    _log(gui, log_fh, "  [ASK] CSV format selected.", "info")

    page.keyboard.press("Tab")
    time.sleep(0.2)

    if not _tab_to_execute():
        _log(gui, log_fh, "  [ASK] Could not reach Execute — aborting.", "err")
        return

    ASK_OUT_DIR.mkdir(parents=True, exist_ok=True)
    _log(gui, log_fh, "  [ASK] Pressing Execute…", "info")
    page.keyboard.press("Enter")
    _log(gui, log_fh, "  [ASK] Execute pressed — waiting for results to load…", "info")
    try:
        page.wait_for_load_state("networkidle", timeout=20_000)
    except PWTimeout:
        pass
    time.sleep(2)

    _log(gui, log_fh, "  [ASK] Waiting for results and download button…", "info")
    deadline = time.time() + 60
    while time.time() < deadline:
        time.sleep(2)
        if "sim_card_download" in page.content():
            break
    else:
        _log(gui, log_fh, "  [ASK] Results did not appear within 60s.", "err")
        return

    _log(gui, log_fh, "  [ASK] Results ready — tabbing to download button…", "info")
    ASK_OUT_DIR.mkdir(parents=True, exist_ok=True)
    page.evaluate("() => { document.body.focus(); }")
    time.sleep(0.3)

    try:
        with page.expect_download(timeout=30_000) as dl_info:
            if not _tab_to_text("sim_card_download"):
                _log(gui, log_fh, "  [ASK] Could not find download button.", "err")
                return
            page.keyboard.press("Enter")
            _log(gui, log_fh, "  [ASK] Download triggered — saving file…", "info")

        download  = dl_info.value
        save_path = ASK_OUT_DIR / download.suggested_filename
        download.save_as(save_path)
        _log(gui, log_fh, f"  [ASK] ✓ Downloaded: {save_path.name}", "ok")

        # Upload to GCS: winrich / Datawarehouse/ASK/YYYY/MM/DD/ask_pms.csv
        try:
            from agents.gcs_storage_agent import GCSStorageAgent
            from agents.base import AgentStatus as _AS
            _t           = date.today() - timedelta(days=1)
            gcs_filename = "ask_pms.csv"
            gcs_prefix   = f"Datawarehouse/ASK/{_t.year}/{_t.month:02d}/{_t.day:02d}"
            gcs          = GCSStorageAgent()
            result       = gcs.run("upload_csv", {
                "file_path":   str(save_path),
                "filename":    gcs_filename,
                "bucket_name": "winrich",
                "prefix":      gcs_prefix,
            })
            if result.status == _AS.SUCCESS:
                _log(gui, log_fh, f"  [ASK] ✓ Uploaded to GCS: {result.output.get('gcs_uri')}", "ok")
                try:
                    save_path.unlink()
                    _log(gui, log_fh, f"  [ASK] Local file removed: {save_path.name}", "info")
                except Exception as del_exc:
                    _log(gui, log_fh, f"  [ASK] Could not remove local file: {del_exc}", "warn")
            else:
                _log(gui, log_fh, f"  [ASK] GCS upload failed: {result.error}", "err")
        except Exception as exc:
            _log(gui, log_fh, f"  [ASK] GCS upload error: {exc}", "err")

    except PWTimeout:
        _log(gui, log_fh, "  [ASK] Download did not complete within 30s.", "err")


def _download_vested(page, gui: RunnerGUI, log_fh) -> None:
    """
    Phase 2 — Log in to Vested Metabase and download the portfolio report.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    VESTED_OUT_DIR.mkdir(parents=True, exist_ok=True)

    email    = os.environ.get("VESTED_EMAIL",    "").strip()
    password = os.environ.get("VESTED_PASSWORD", "").strip()
    if not email or not password:
        _log(gui, log_fh, "  [Vested] VESTED_EMAIL or VESTED_PASSWORD not set — skipping.", "err")
        return

    _log(gui, log_fh, "\n► [Vested] Navigating to login page…", "info")
    try:
        page.goto(URL_VESTED_LOGIN, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass

    current_url = page.url
    _log(gui, log_fh, f"  [Vested] Landed on: {current_url}", "info")

    # Skip login if already authenticated
    if "login" not in current_url:
        _log(gui, log_fh, "  [Vested] Already logged in — skipping login.", "ok")
    else:
        _log(gui, log_fh, f"  [Vested] Logging in as {email}…", "info")

        # Fill email
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
            email_field = page.locator(
                "input[type='email'], input[name='username'], input[name='email'], "
                "input[type='text']:visible"
            ).first
            email_field.wait_for(state="visible", timeout=15_000)
            email_field.click()
            email_field.fill(email)
        except PWTimeout:
            _log(gui, log_fh, "  [Vested] Could not find email field — dumping inputs.", "err")
            inputs = page.evaluate("""() => Array.from(document.querySelectorAll('input')).map(el => ({
                type: el.type, name: el.name, id: el.id, placeholder: el.placeholder,
                visible: el.offsetParent !== null,
            }))""")
            for inp in inputs:
                _log(gui, log_fh, f"    {inp}", "info")
            return

        # Fill password
        try:
            pwd_field = page.locator("input[type='password']").first
            pwd_field.wait_for(state="visible", timeout=5_000)
            pwd_field.fill(password)
        except PWTimeout:
            _log(gui, log_fh, "  [Vested] Could not find password field.", "err")
            return

        # Submit
        page.keyboard.press("Enter")
        _log(gui, log_fh, "  [Vested] Credentials submitted.", "info")
        try:
            page.wait_for_url(lambda u: "login" not in u, timeout=30_000)
            _log(gui, log_fh, f"  [Vested] Logged in — Page: {page.url}", "ok")
        except PWTimeout:
            _log(gui, log_fh, f"  [Vested] Login may not have completed. URL: {page.url}", "warn")

    # ── Navigate directly to Funded Users question ────────────────────────────
    VESTED_QUESTION_URL = "https://metabase-partners.vestedfinance.com/question/364-funded-users?Time_Limit=12&Time_Frame=Month"
    _log(gui, log_fh, f"  [Vested] Navigating to Funded Users report…", "info")
    try:
        page.goto(VESTED_QUESTION_URL, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=20_000)
    except PWTimeout:
        pass
    time.sleep(2)
    _log(gui, log_fh, f"  [Vested] Page: {page.url}", "info")

    # ── Click the download button (bottom-right corner of the Metabase UI) ────
    # Metabase renders a download icon button; try common selectors.
    _log(gui, log_fh, "  [Vested] Looking for download button…", "info")
    download_sel = (
        "button[data-testid='download-button'], "
        "button.Icon-download, "
        "[aria-label*='download' i], "
        "[aria-label*='export' i], "
        ".Icon-download"
    )
    try:
        dl_btn = page.locator(download_sel).first
        dl_btn.wait_for(state="visible", timeout=15_000)
        dl_btn.click()
        time.sleep(1)
        _log(gui, log_fh, "  [Vested] Download menu opened.", "info")
    except PWTimeout:
        _log(gui, log_fh, "  [Vested] Download button not found — dumping visible buttons.", "warn")
        btns = page.evaluate("""() => Array.from(document.querySelectorAll('button, a')).map(el => ({
            text: el.innerText.trim().substring(0, 60),
            aria: el.getAttribute('aria-label') || '',
            cls:  el.className.substring(0, 80),
            visible: el.offsetParent !== null,
        })).filter(b => b.visible && (b.text || b.aria))""")
        for b in btns[-20:]:   # bottom of the page buttons
            _log(gui, log_fh, f"    text={b['text']!r} aria={b['aria']!r} cls={b['cls']!r}", "info")
        return

    # ── Select .csv format then click Download ────────────────────────────────
    # The panel shows: .csv / .xlsx / .json spans, then a "Download" button.
    VESTED_OUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        # Step 1: click the .csv span to select CSV format
        csv_opt = page.locator("span:text-is('.csv')").first
        csv_opt.wait_for(state="visible", timeout=10_000)
        csv_opt.click()
        time.sleep(0.3)
        _log(gui, log_fh, "  [Vested] Selected .csv format.", "info")

        # Step 2: click the Download button to trigger the file download
        dl_btn = page.locator("button:has(span.mb-mantine-Button-label:text-is('Download'))").first
        dl_btn.wait_for(state="visible", timeout=5_000)
        with page.expect_download(timeout=60_000) as dl_info:
            dl_btn.click()
        download  = dl_info.value
        save_path = VESTED_OUT_DIR / download.suggested_filename
        download.save_as(save_path)
        _log(gui, log_fh, f"  [Vested] ✓ Downloaded: {save_path.name}", "ok")

        # Upload to GCS: winrich / Datawarehouse/Vested/YYYY/MM/DD/funded_users_YYYY-MM-DD.csv
        try:
            from agents.gcs_storage_agent import GCSStorageAgent
            from agents.base import AgentStatus as _AS
            _t = date.today()
            gcs_filename = f"funded_users_{_t.strftime('%Y-%m-%d')}.csv"
            gcs_prefix   = f"Datawarehouse/Vested/{_t.year}/{_t.month:02d}/{_t.day:02d}"
            gcs = GCSStorageAgent()
            result = gcs.run("upload_csv", {
                "file_path":   str(save_path),
                "filename":    gcs_filename,
                "bucket_name": "winrich",
                "prefix":      gcs_prefix,
            })
            if result.status == _AS.SUCCESS:
                _log(gui, log_fh, f"  [Vested] ✓ Uploaded to GCS: {result.output.get('gcs_uri')}", "ok")
                try:
                    save_path.unlink()
                    _log(gui, log_fh, f"  [Vested] Local file removed: {save_path.name}", "info")
                except Exception as del_exc:
                    _log(gui, log_fh, f"  [Vested] Could not remove local file: {del_exc}", "warn")
            else:
                _log(gui, log_fh, f"  [Vested] GCS upload failed: {result.error}", "err")
        except Exception as exc:
            _log(gui, log_fh, f"  [Vested] GCS upload error: {exc}", "err")

    except PWTimeout as e:
        _log(gui, log_fh, f"  [Vested] Download failed: {e}", "err")


def _download_ipru_emails(gui: RunnerGUI, log_fh) -> None:
    """
    Phase 3 — Download latest ICICI PMS report from IPRU email.

    Flow:
      1. Find the most recent email from IPRU_SENDER.
      2. Download the WAWYA_IN*.xls attachment.
      3. Convert XLS → CSV named ICICIPMS_YYYY-MM-DD.csv.
      4. Upload to GCS: winrich / Datawarehouse/ICICI-PMS/YYYY/MM/DD/
      5. Delete local files.
    """
    if not _OUTLOOK_AGENT_OK:
        _log(gui, log_fh, "\n► [IPRU Email] OutlookInboxAgent unavailable — skipping.", "err")
        return

    from agents.outlook_inbox_agent import OutlookInboxAgent
    from agents.base import AgentStatus

    import fnmatch
    import pandas as pd

    IPRU_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    _log(gui, log_fh, f"\n► [IPRU Email] Searching for latest email from {IPRU_SENDER}…", "info")

    agent          = OutlookInboxAgent()
    received_after = (date.today() - timedelta(days=IPRU_LOOKBACK_DAYS)).isoformat()

    search_result = agent.run("search_emails", {
        "sender_email":   IPRU_SENDER,
        "received_after": received_after,
        "top": 50,
    })

    if search_result.status != AgentStatus.SUCCESS:
        _log(gui, log_fh, f"  [IPRU Email] Search failed: {search_result.error}", "err")
        return

    messages = search_result.output.get("messages", [])
    messages = [m for m in messages if m["has_attachments"]]
    if not messages:
        _log(gui, log_fh, "  [IPRU Email] No emails with attachments found.", "warn")
        return

    # Use the most recent email
    messages.sort(key=lambda m: m.get("received_at", ""), reverse=True)
    msg      = messages[0]
    msg_date = msg["received_at"][:10]
    _log(gui, log_fh, f"  [IPRU Email] Using email: {msg_date}  {msg['subject']}", "info")

    # List attachments and find WAWYA_IN*.xls
    att_result = agent.run("list_attachments", {"message_id": msg["id"]})
    if att_result.status != AgentStatus.SUCCESS:
        _log(gui, log_fh, f"  [IPRU Email] Could not list attachments: {att_result.error}", "err")
        return

    attachments = att_result.output.get("attachments", [])

    # Accept WAWYA_IN*.xls directly OR a ZIP containing it
    target_att = next(
        (a for a in attachments
         if fnmatch.fnmatch(a["name"].upper(), "WAWYA_IN*.XLS")
         or fnmatch.fnmatch(a["name"].upper(), "WAWYA IN*.XLS")
         or fnmatch.fnmatch(a["name"].upper(), "WAWYA*.ZIP")
         or fnmatch.fnmatch(a["name"].upper(), "WAWYA_IN*.ZIP")),
        None,
    )
    if target_att is None:
        names = [a["name"] for a in attachments]
        _log(gui, log_fh, f"  [IPRU Email] No WAWYA attachment found. Attachments: {names}", "warn")
        return

    _log(gui, log_fh, f"  [IPRU Email] Downloading: {target_att['name']} "
                      f"({target_att['size_kb']:.1f} KB)…", "info")

    dl = agent.run("download_attachment", {
        "message_id":    msg["id"],
        "attachment_id": target_att["id"],
        "file_name":     target_att["name"],
        "save_dir":      str(IPRU_DOWNLOAD_DIR),
    })
    if dl.status != AgentStatus.SUCCESS:
        _log(gui, log_fh, f"  [IPRU Email] Download failed: {dl.error}", "err")
        return

    downloaded_path = Path(dl.output["saved_path"])
    _log(gui, log_fh, f"  [IPRU Email] ✓ Downloaded: {downloaded_path.name}", "ok")

    # If it's a ZIP, extract and find the XLS inside
    local_files_to_delete = [downloaded_path]
    if downloaded_path.suffix.upper() == ".ZIP":
        import zipfile
        _log(gui, log_fh, f"  [IPRU Email] Extracting ZIP…", "info")
        with zipfile.ZipFile(downloaded_path) as zf:
            xls_members = [m for m in zf.namelist()
                           if fnmatch.fnmatch(m.upper(), "WAWYA_IN*.XLS")
                           or fnmatch.fnmatch(m.upper(), "WAWYA IN*.XLS")]
            if not xls_members:
                _log(gui, log_fh, f"  [IPRU Email] No WAWYA_IN*.xls inside ZIP. Contents: {zf.namelist()}", "warn")
                return
            xls_member = xls_members[0]
            zf.extract(xls_member, IPRU_DOWNLOAD_DIR)
            xls_path = IPRU_DOWNLOAD_DIR / xls_member
            local_files_to_delete.append(xls_path)
        _log(gui, log_fh, f"  [IPRU Email] Extracted: {xls_path.name}", "ok")
    else:
        xls_path = downloaded_path

    # Convert XLS → CSV
    _t       = date.today()
    csv_name = f"ICICIPMS {_t.day}.csv"
    csv_path = IPRU_DOWNLOAD_DIR / csv_name
    local_files_to_delete.append(csv_path)
    try:
        df = pd.read_excel(xls_path, engine="xlrd")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        _log(gui, log_fh, f"  [IPRU Email] ✓ Converted to CSV: {csv_name} ({len(df)} rows)", "ok")
    except Exception as exc:
        _log(gui, log_fh, f"  [IPRU Email] XLS→CSV conversion failed: {exc}", "err")
        return

    # Upload to GCS: winrich / Datawarehouse/ICICI-PMS/YYYY/MM/DD/
    try:
        from agents.gcs_storage_agent import GCSStorageAgent
        from agents.base import AgentStatus as _AS
        gcs_prefix = f"Datawarehouse/ICICI-PMS/{_t.year}/{_t.month:02d}/{_t.day:02d}"
        gcs        = GCSStorageAgent()
        gcs_result = gcs.run("upload_csv", {
            "file_path":   str(csv_path),
            "filename":    csv_name,
            "bucket_name": "winrich",
            "prefix":      gcs_prefix,
        })
        if gcs_result.status == _AS.SUCCESS:
            _log(gui, log_fh,
                 f"  [IPRU Email] ✓ Uploaded to GCS: {gcs_result.output.get('gcs_uri')}", "ok")
            # Delete all local files after successful upload
            for p in local_files_to_delete:
                try:
                    p.unlink()
                    _log(gui, log_fh, f"  [IPRU Email] Local file removed: {p.name}", "info")
                except Exception as del_exc:
                    _log(gui, log_fh, f"  [IPRU Email] Could not remove {p.name}: {del_exc}", "warn")
        else:
            _log(gui, log_fh, f"  [IPRU Email] GCS upload failed: {gcs_result.error}", "err")
    except Exception as exc:
        _log(gui, log_fh, f"  [IPRU Email] GCS upload error: {exc}", "err")


def _fetch_angelone_otp_UNUSED(gui: RunnerGUI, log_fh, triggered_at: float) -> str | None:
    """
    Poll MS_GRAPH_MAILBOX for an OTP email from Angel One sent after triggered_at.
    Returns the OTP string or None on timeout.
    """
    if not _OUTLOOK_AGENT_OK:
        _log(gui, log_fh, "  [AO OTP] OutlookInboxAgent unavailable.", "err")
        return None

    from agents.outlook_inbox_agent import OutlookInboxAgent
    from agents.base import AgentStatus
    from datetime import datetime

    agent    = OutlookInboxAgent()
    mailbox  = os.environ.get("MS_GRAPH_MAILBOX", "").strip().strip('"')
    max_wait = 300
    interval = 10

    _log(gui, log_fh, f"  [AO OTP] Waiting for OTP from {ANGELONE_OTP_SENDER} in {mailbox}…", "info")

    for attempt in range(max_wait // interval):
        time.sleep(interval)
        elapsed = (attempt + 1) * interval
        _log(gui, log_fh, f"  [AO OTP] Checking inbox (attempt {attempt + 1}, {elapsed}s elapsed)…", "info")

        result = agent.run("search_emails", {
            "mailbox":        mailbox,
            "received_after": date.today().isoformat(),
            "top":            50,
        })
        if result.status != AgentStatus.SUCCESS:
            _log(gui, log_fh, f"  [AO OTP] Search error: {result.error}", "warn")
            continue

        all_msgs = result.output.get("messages", [])
        messages = [m for m in all_msgs
                    if m.get("sender_email", "").lower() == ANGELONE_OTP_SENDER.lower()]
        messages.sort(key=lambda m: m.get("received_at", ""), reverse=True)

        for msg in messages:
            received_str = msg.get("received_at", "")
            preview      = msg.get("preview", "")
            try:
                received_ts = datetime.fromisoformat(
                    received_str.replace("Z", "+00:00")
                ).timestamp()
            except ValueError:
                received_ts = 0

            if received_ts < triggered_at - 30:
                _log(gui, log_fh, "  [AO OTP] Email older than login attempt — skipping.", "info")
                continue

            match = re.search(r'\b(\d{4,8})\b', preview)
            if match:
                otp = match.group(1)
                _log(gui, log_fh, f"  [AO OTP] ✓ OTP found: {otp}", "ok")
                return otp

    _log(gui, log_fh, "  [AO OTP] OTP not received within timeout.", "err")
    return None


def _login_angelone(page, gui: RunnerGUI, log_fh) -> bool:
    """
    Automate Angel One NXT login: enter credentials, wait for OTP, submit.
    Returns True if login succeeded.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    user_id  = os.environ.get("ANGELONE_USERID",   "").strip()
    password = os.environ.get("ANGELONE_PASSWORD",  "").strip()
    if not user_id or not password:
        _log(gui, log_fh, "  [AO Login] ANGELONE_USERID or ANGELONE_PASSWORD not set in .env.", "err")
        return False

    _log(gui, log_fh, f"  [AO Login] Entering credentials for {user_id}…", "info")

    # ── User ID ───────────────────────────────────────────────────────────────
    try:
        uid_field = page.locator("input[type='text'], input[placeholder*='Client'], input[placeholder*='User'], input[name*='client'], input[name*='user']").first
        uid_field.wait_for(state="visible", timeout=10_000)
        uid_field.click()
        uid_field.fill(user_id)
    except PWTimeout:
        _log(gui, log_fh, "  [AO Login] Could not find user ID field — dumping inputs.", "err")
        inputs = page.evaluate("""() => Array.from(document.querySelectorAll('input')).map(el => ({
            type: el.type, name: el.name, id: el.id, placeholder: el.placeholder, visible: el.offsetParent !== null
        }))""")
        for inp in inputs:
            _log(gui, log_fh, f"    {inp}", "info")
        return False

    # ── Password ──────────────────────────────────────────────────────────────
    try:
        pwd_field = page.locator("input[type='password']").first
        pwd_field.wait_for(state="visible", timeout=5_000)
        pwd_field.click()
        pwd_field.fill(password)
    except PWTimeout:
        _log(gui, log_fh, "  [AO Login] Could not find password field.", "err")
        return False

    triggered_at = time.time()
    page.keyboard.press("Enter")
    _log(gui, log_fh, "  [AO Login] Credentials submitted — waiting for OTP prompt…", "info")

    # ── Wait for OTP input to appear ──────────────────────────────────────────
    try:
        otp_field = page.locator(
            "input[placeholder*='OTP'], input[placeholder*='otp'], "
            "input[name*='otp'], input[name*='OTP'], "
            "input[maxlength='6'], input[maxlength='4']"
        ).first
        otp_field.wait_for(state="visible", timeout=30_000)
    except PWTimeout:
        _log(gui, log_fh, "  [AO Login] OTP field did not appear — dumping inputs.", "err")
        inputs = page.evaluate("""() => Array.from(document.querySelectorAll('input')).map(el => ({
            type: el.type, name: el.name, id: el.id, placeholder: el.placeholder,
            maxlength: el.maxLength, visible: el.offsetParent !== null
        }))""")
        for inp in inputs:
            _log(gui, log_fh, f"    {inp}", "info")
        return False

    # ── Fetch OTP from email ───────────────────────────────────────────────────
    otp = _fetch_angelone_otp(gui, log_fh, triggered_at)
    if not otp:
        _log(gui, log_fh, "  [AO Login] Could not obtain OTP — login aborted.", "err")
        return False

    otp_field.click()
    otp_field.fill(otp)
    page.keyboard.press("Enter")
    _log(gui, log_fh, f"  [AO Login] OTP submitted: {otp}", "info")

    # ── Wait for redirect away from /auth ─────────────────────────────────────
    try:
        page.wait_for_url(
            lambda url: "nxt.angelone.in" in url and "/auth" not in url,
            timeout=30_000,
        )
        _log(gui, log_fh, "  [AO Login] ✓ Logged in successfully.", "ok")
        return True
    except PWTimeout:
        _log(gui, log_fh, f"  [AO Login] Login did not complete. URL: {page.url}", "err")
        return False


def _dismiss_popup(page, gui: RunnerGUI) -> None:
    from playwright.sync_api import TimeoutError as PWTimeout
    for _ in range(3):
        try:
            btn = page.locator("button:has-text('LATER'), button:has-text('Later')").first
            btn.wait_for(state="visible", timeout=2_000)
            btn.click()
            gui.log_line("  [popup] Dismissed.", "info")
            time.sleep(0.5)
            return
        except PWTimeout:
            time.sleep(1)


def _download_report(page, report_name: str, csv_name: str, gui: RunnerGUI,
                     gcs_bucket: str = "winrich_shared",
                     gcs_filename: str = None) -> None:
    from playwright.sync_api import TimeoutError as PWTimeout
    from agents.gcs_storage_agent import GCSStorageAgent
    from agents.base import AgentStatus
    import pandas as pd

    today_str = date.today().strftime("%b %d %Y")
    gui.log_line(f"\n► Downloading '{report_name}'…", "info")

    MAX_ATTEMPTS = 3
    xlsx_path    = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        if attempt > 1:
            gui.log_line(f"  [retry] Attempt {attempt}/{MAX_ATTEMPTS} — refreshing page…", "warn")
            time.sleep(2)

        page.goto(URL_DOWNLOADS, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=10_000)
        _dismiss_popup(page, gui)

        MAX_TABS = 60
        found    = False
        page.keyboard.press("Tab")

        for i in range(MAX_TABS):
            time.sleep(0.3)
            label = page.evaluate("""() => {
                const el = document.activeElement;
                if (!el) return null;
                const txt = (el.innerText || el.textContent || '').trim();
                let card = el.parentElement;
                let cardText = '';
                for (let j = 0; j < 15; j++) {
                    if (!card) break;
                    cardText = card.innerText || card.textContent || '';
                    if (cardText.length > 30) break;
                    card = card.parentElement;
                }
                return {focused: txt, card: cardText};
            }""")

            if label and label["focused"] == "DOWNLOAD":
                if report_name in label["card"] and today_str in label["card"]:
                    gui.log_line(f"  [found] Tab #{i+1}: DOWNLOAD for '{report_name}'", "ok")
                    found = True
                    break

            page.keyboard.press("Tab")

        if not found:
            gui.log_line(
                f"  [warn] Attempt {attempt}: DOWNLOAD button not found for '{report_name}' "
                f"({today_str}) — banner may be blocking.", "warn")
            continue

        try:
            with page.expect_download(timeout=60_000) as dl_info:
                page.evaluate("() => document.activeElement.click()")
            download  = dl_info.value
            xlsx_path = OUT_DIR / download.suggested_filename
            download.save_as(xlsx_path)
            gui.log_line(f"  [downloaded] {download.suggested_filename}", "ok")
            break   # success — exit retry loop
        except Exception as exc:
            gui.log_line(f"  [warn] Attempt {attempt}: download error — {exc}", "warn")
            xlsx_path = None

    if xlsx_path is None:
        gui.log_line(
            f"  [skip] '{report_name}' — could not download after {MAX_ATTEMPTS} attempts. "
            f"Moving to next step.", "err")
        return

    csv_path = OUT_DIR / csv_name
    # Angel One reports have a title row before the actual column headers.
    # Detect the real header row by finding the first row where >50% of cells
    # are non-empty strings (i.e. actual column names, not a sparse title row).
    raw = pd.read_excel(xlsx_path, engine="openpyxl", header=None)
    header_row = 0
    for i, row in raw.iterrows():
        non_empty = row.dropna().astype(str).str.strip().str.len().gt(0).sum()
        if non_empty > len(row) * 0.5:
            header_row = i
            break
    df = pd.read_excel(xlsx_path, engine="openpyxl", skiprows=header_row)
    df.columns = df.columns.str.replace("₹", "?", regex=False).str.strip()
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    gui.log_line(f"  [converted] {len(df)} rows → {csv_name} (header at row {header_row})", "ok")


    today = date.today()
    gcs_prefix = f"Datawarehouse/Stocks/{today.year}/{today.strftime('%m')}/{today.strftime('%d')}"
    gcs_resp = GCSStorageAgent().run("upload_csv", {
        "file_path":   str(csv_path),
        "filename":    gcs_filename or csv_name,
        "bucket_name": gcs_bucket,
        "prefix":      gcs_prefix,
    })
    if gcs_resp.status == AgentStatus.SUCCESS:
        gui.log_line(f"  [gcs] ✓ {gcs_resp.output['gcs_uri']}", "ok")
    else:
        gui.log_line(f"  [gcs] Upload failed: {gcs_resp.error}", "err")


def _focus_top_search(page) -> bool:
    page.evaluate("() => { document.body.focus(); }")
    time.sleep(0.2)
    page.keyboard.press("Tab")
    time.sleep(0.3)
    info = page.evaluate("""() => {
        const el = document.activeElement;
        return {tag: el.tagName, placeholder: el.placeholder || ''};
    }""")
    if info["tag"] == "INPUT":
        return True
    page.keyboard.press("Tab")
    time.sleep(0.3)
    info = page.evaluate("""() => {
        const el = document.activeElement;
        return {tag: el.tagName, placeholder: el.placeholder || ''};
    }""")
    return info["tag"] == "INPUT"


def _search_customer(page, username: str) -> bool:
    if not _focus_top_search(page):
        return False

    page.keyboard.press("Control+a")
    page.keyboard.type(username, delay=60)
    time.sleep(2.0)

    page.keyboard.press("ArrowDown")
    time.sleep(0.3)

    tag, text = page.evaluate("""() => {
        const el = document.activeElement;
        return [el.tagName, (el.innerText || el.textContent || '').trim()];
    }""")

    if tag == "INPUT" or not text:
        page.goto(URL_DASHBOARD, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=10_000)
        return False

    page.evaluate("() => document.activeElement.click()")
    time.sleep(0.5)
    page.keyboard.press("Enter")
    time.sleep(2.0)
    return True


def _extract_customer_data(page) -> dict:
    time.sleep(1.0)
    raw = page.evaluate("""() => {
        const FIELDS = [
            'AUM (Equity)', 'AUM (MF)', 'AUM (Bonds)',
            'Margin Available for Trade', 'Accrual',
            'Net Ledger Balance', 'T-1 Equity Portfolio',
        ];
        function findValue(label) {
            let bestVal = null;
            let bestLen = Infinity;
            const walker = document.createTreeWalker(
                document.body, NodeFilter.SHOW_ELEMENT, null
            );
            while (walker.nextNode()) {
                const el = walker.currentNode;
                const text = (el.innerText || el.textContent || '').trim();

                // Exact label match: look for value in siblings
                if (text === label) {
                    let sib = el.nextElementSibling;
                    if (sib) {
                        const val = (sib.innerText || sib.textContent || '').trim();
                        if (val) return val;
                    }
                    const parentSib = el.parentElement && el.parentElement.nextElementSibling;
                    if (parentSib) {
                        const val = (parentSib.innerText || parentSib.textContent || '').trim();
                        if (val) return val;
                    }
                    const gp = el.parentElement && el.parentElement.parentElement;
                    const gpSib = gp && gp.nextElementSibling;
                    if (gpSib) {
                        const val = (gpSib.innerText || gpSib.textContent || '').trim();
                        if (val) return val;
                    }
                }

                // Fallback: label and value combined in same element
                // Track the smallest matching element to avoid grabbing a large container
                if (text.startsWith(label) && text.length > label.length && text.length < bestLen) {
                    const rest = text.slice(label.length).trim();
                    if (rest) { bestVal = rest; bestLen = text.length; }
                }
            }
            return bestVal;
        }
        const result = {};
        for (const f of FIELDS) result[f] = findValue(f);
        return result;
    }""")

    # Convert all monetary values to plain floats
    return {k: _clean_amount(v) for k, v in raw.items()}


# ══════════════════════════════════════════════════════════════════════════════
# Main automation loop (background thread)
# ══════════════════════════════════════════════════════════════════════════════

def _send_report_email(df, total: int, captured: int, errors: list) -> str:
    """Send scraped data as an HTML table to EMAIL_TO. Returns status string."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    today_str = date.today().strftime("%d %b %Y")

    # ── Build HTML table ───────────────────────────────────────────────────
    # Show only the key columns in the email
    display_cols = [c for c in [
        "master_customer_id", "name", "date",
        "AUM (Equity)", "AUM (MF)", "AUM (Bonds)",
        "Margin Available for Trade", "Accrual",
        "Net Ledger Balance", "T-1 Equity Portfolio",
    ] if c in df.columns]
    table_df = df[display_cols].copy()

    # Format numeric columns as ₹ values
    for col in table_df.columns:
        if col not in ("master_customer_id", "name", "date"):
            table_df[col] = table_df[col].apply(
                lambda v: f"₹{v:,.0f}" if v is not None and str(v) not in ("", "nan") and str(v) != "None"
                else "—"
            )

    th_style = (
        "background:#1a2a5e;color:#fff;padding:6px 10px;"
        "text-align:left;font-size:12px;white-space:nowrap;"
    )
    td_style = "padding:5px 10px;font-size:12px;border-bottom:1px solid #ddd;"
    tr_alt   = "background:#f4f6fb;"

    header_html = "".join(f"<th style='{th_style}'>{c}</th>" for c in table_df.columns)
    rows_html   = ""
    for i, (_, row) in enumerate(table_df.iterrows()):
        tr_style = f"style='{tr_alt}'" if i % 2 == 1 else ""
        cells = "".join(f"<td style='{td_style}'>{v}</td>" for v in row)
        rows_html += f"<tr {tr_style}>{cells}</tr>"

    html = f"""
    <html><body style="font-family:Arial,sans-serif;color:#222;">
    <h2 style="color:#1a2a5e;">ANGEL NXT — Daily Scrape Report</h2>
    <p style="color:#555;">Date: <b>{today_str}</b> &nbsp;|&nbsp;
       Customers scraped: <b>{captured}/{total}</b>
       {"&nbsp;|&nbsp;<span style='color:red'>Errors: " + str(len(errors)) + "</span>" if errors else ""}
    </p>
    <table border="0" cellspacing="0" cellpadding="0"
           style="border-collapse:collapse;width:100%;min-width:600px;">
      <thead><tr>{header_html}</tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    {"<p style='color:red;margin-top:12px;'>Failed customers: " + ", ".join(errors) + "</p>" if errors else ""}
    <p style="color:#aaa;font-size:11px;margin-top:16px;">
      Sent automatically by ANGEL NXT Daily Runner.
    </p>
    </body></html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"ANGEL NXT Daily Report — {today_str}"
    msg["From"]    = EMAIL_FROM
    msg["To"]      = EMAIL_TO
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_FROM, EMAIL_APP_PWD.replace(" ", ""))
        smtp.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())

    return f"Email sent to {EMAIL_TO}"


def _save_appended(new_df) -> None:
    """Append new_df to OUT_CSV, deduplicating on (date, master_customer_id)."""
    import pandas as pd
    if OUT_CSV.exists():
        existing = pd.read_csv(OUT_CSV)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined.drop_duplicates(subset=["date", "master_customer_id"], keep="last", inplace=True)
    else:
        combined = new_df
    combined.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")


def _setup_log_file():
    """Open a dated log file in data/ and return it."""
    OUT_DIR.mkdir(exist_ok=True)
    log_path = OUT_DIR / f"run_{date.today().isoformat()}.log"
    return open(log_path, "a", encoding="utf-8")


def _log(gui: RunnerGUI, fh, text: str, tag: str = "") -> None:
    """Write to GUI and log file simultaneously."""
    gui.log_line(text, tag)
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {text}", file=fh, flush=True)


def _run(gui: RunnerGUI) -> None:
    import pandas as pd
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    OUT_DIR.mkdir(exist_ok=True)
    log_fh = _setup_log_file()

    customers = pd.read_csv(IN_CSV, encoding="utf-8-sig")
    # Normalise column names and filter to Angel One customers only
    customers.columns = customers.columns.str.strip()

    # Strip BOM (both \ufeff and its Latin-1 representation ï»¿) then rename
    customers.columns = (customers.columns
                         .str.replace('\ufeff', '', regex=False)
                         .str.replace('ï»¿',   '', regex=False)
                         .str.strip())
    customers = customers.rename(columns={
        "Customer ID":   "master_customer_id",
        "Customer Name": "username",
    })

    customers = customers[customers["Broker Type"].str.strip() == "Angel One"].reset_index(drop=True)
    if TEST_CUSTOMER:
        customers = customers[customers["username"].str.lower() == TEST_CUSTOMER.lower()]
        _log(gui, log_fh, f"(TEST MODE — single customer: {TEST_CUSTOMER!r})", "warn")
    elif TEST_LIMIT:
        customers = customers.head(TEST_LIMIT)
        _log(gui, log_fh, f"(TEST MODE — first {TEST_LIMIT} only)", "warn")
    total = len(customers)
    _log(gui, log_fh, f"Loaded {total} customers from {IN_CSV.name}", "info")

    # ── 15-second grace period — user can uncheck completed phases ───────────
    _COUNTDOWN = 15
    _log(gui, log_fh, f"Starting in {_COUNTDOWN}s — uncheck any phases already complete.", "warn")
    for _remaining in range(_COUNTDOWN, 0, -1):
        gui.set_status(f"Starting in {_remaining}s — uncheck phases already complete…")
        time.sleep(1)
    gui.set_status("Starting…")
    _log(gui, log_fh, "Grace period over — starting run.", "info")

    while True:   # retry loop if Edge is closed mid-run

        # Close any leftover Edge process gracefully before launching fresh
        subprocess.run(["taskkill", "/IM", "msedge.exe"], capture_output=True)
        time.sleep(3)   # give Edge time to close cleanly
        # Force-kill only if it didn't close in time
        if _edge_running():
            subprocess.run(["taskkill", "/F", "/IM", "msedge.exe"], capture_output=True)
            time.sleep(1)

        gui.set_status("Launching browser…")
        _log(gui, log_fh, f"\nLaunching Edge from: {EDGE_EXE}", "info")
        edge_proc = subprocess.Popen([
            EDGE_EXE,
            f"--remote-debugging-port={CDP_PORT}",
            f"--user-data-dir={PROFILE_DIR}",
            "--no-first-run",
            "--no-default-browser-check",
        ])
        _log(gui, log_fh, f"Edge PID: {edge_proc.pid}", "info")

        # Wait for Edge CDP to be ready — slower machines need more time
        ready = False
        for _ in range(20):
            time.sleep(1)
            if _edge_running():
                ready = True
                break
        if not ready:
            raise RuntimeError(
                "Edge launched but CDP port not ready after 20s. "
                "Check Edge is installed correctly."
            )

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.connect_over_cdp(CDP_URL)
                context = browser.contexts[0] if browser.contexts else browser.new_context()
                pages   = context.pages
                page    = pages[0] if pages else context.new_page()

                # ── Open AUM page first — login only if redirected ────────
                _log(gui, log_fh, f"Opening Unify AUM page: {URL_UNIFY_AUM}", "info")
                try:
                    page.goto(URL_UNIFY_AUM, wait_until="commit", timeout=30_000)
                except Exception:
                    pass
                gui.set_status("Browser open — Unify loading…")
                gui.start_progress()

                _force = gui.download_files_var.get()

                # ── Phase 1: Unify ─────────────────────────────────────────
                if gui.skip_unify_var.get():
                    _log(gui, log_fh, "\n► [Unify] Skipped (checkbox).", "warn")
                else:
                    _t1 = date.today() - timedelta(days=1)
                    _unify_blob = (f"Datawarehouse/Unify/{_t1.year}/{_t1.month:02d}/{_t1.day:02d}"
                                   f"/WAWYA_Daily_AUM_{_t1.strftime('%d-%m-%Y')}.csv")
                    if not _force and _gcs_blob_exists("winrich", _unify_blob, gui, log_fh):
                        _log(gui, log_fh, f"\n► [Unify] Skipped — already in GCS: {_unify_blob}", "warn")
                    else:
                        gui.set_status("Phase 1 — Downloading Unify data…")
                        _download_unify(page, gui, log_fh)

                # ── Phase 2: ASK ───────────────────────────────────────────
                if gui.skip_ask_var.get():
                    _log(gui, log_fh, "\n► [ASK] Skipped (checkbox).", "warn")
                else:
                    _t2 = date.today() - timedelta(days=1)
                    _ask_blob = (f"Datawarehouse/ASK/{_t2.year}/{_t2.month:02d}/{_t2.day:02d}"
                                 f"/ask_pms.csv")
                    if not _force and _gcs_blob_exists("winrich", _ask_blob, gui, log_fh):
                        _log(gui, log_fh, f"\n► [ASK] Skipped — already in GCS: {_ask_blob}", "warn")
                    else:
                        gui.set_status("Phase 2 — Downloading ASK data…")
                        _download_ask(page, gui, log_fh)

                # ── Phase 3: Vested ────────────────────────────────────────
                if gui.skip_vested_var.get():
                    _log(gui, log_fh, "\n► [Vested] Skipped (checkbox).", "warn")
                else:
                    _t3 = date.today()
                    _vested_blob = (f"Datawarehouse/Vested/{_t3.year}/{_t3.month:02d}/{_t3.day:02d}"
                                    f"/funded_users_{_t3.strftime('%Y-%m-%d')}.csv")
                    if not _force and _gcs_blob_exists("winrich", _vested_blob, gui, log_fh):
                        _log(gui, log_fh, f"\n► [Vested] Skipped — already in GCS: {_vested_blob}", "warn")
                    else:
                        gui.set_status("Phase 3 — Downloading Vested data…")
                        _download_vested(page, gui, log_fh)

                # ── Phase 4: IPRU Email (no browser needed) ────────────────
                if gui.skip_email_var.get():
                    _log(gui, log_fh, "\n► [IPRU Email] Skipped (checkbox).", "warn")
                else:
                    _t4 = date.today()
                    _ipru_blob = (f"Datawarehouse/ICICI-PMS/{_t4.year}/{_t4.month:02d}/{_t4.day:02d}"
                                  f"/ICICIPMS {_t4.day}.csv")
                    if not _force and _gcs_blob_exists("winrich", _ipru_blob, gui, log_fh):
                        _log(gui, log_fh, f"\n► [IPRU Email] Skipped — already in GCS: {_ipru_blob}", "warn")
                    else:
                        gui.set_status("Phase 4 — Downloading IPRU email attachments…")
                        _download_ipru_emails(gui, log_fh)

                # ── Phase 4: Angel One NXT ─────────────────────────────────
                if gui.skip_angelone_var.get():
                    _log(gui, log_fh, "\n► [Angel One NXT] Skipped (checkbox).", "warn")
                    gui.stop_progress()
                    gui.set_status("✓ Done — Angel One NXT skipped.")
                    log_fh.close()
                    return

                # Navigate to Angel One NXT — check dashboard first to skip login if session active
                gui.set_status("Phase 5 — Logging in to Angel One NXT…")
                _log(gui, log_fh, "\n► [Angel One NXT] Checking dashboard…", "info")
                try:
                    page.goto(URL_DASHBOARD, wait_until="domcontentloaded", timeout=30_000)
                    page.wait_for_load_state("networkidle", timeout=10_000)
                except Exception:
                    pass

                if "nxt.angelone.in" in page.url and "/auth" not in page.url:
                    _log(gui, log_fh, "Already logged in to Angel One NXT — dashboard loaded.", "ok")
                else:
                    _log(gui, log_fh, "Session expired — need to log in to Angel One NXT.", "info")
                    try:
                        page.goto(URL_AUTH, wait_until="commit", timeout=30_000)
                    except Exception:
                        pass

                    if gui.angelone_auto_login_var.get():
                        # ── Automatic login via credentials + email OTP ────────
                        _log(gui, log_fh, "  [AO Login] Auto-login selected — attempting automated login…", "info")
                        if not _login_angelone(page, gui, log_fh):
                            gui.stop_progress()
                            gui.set_status("✗ Angel One NXT auto-login failed.")
                            log_fh.close()
                            return
                    else:
                        # ── Manual login — wait for user to log in in browser ──
                        _restore_edge()
                        gui.set_status("Phase 5 — Please log in to Angel One NXT in the browser…")
                        _log(gui, log_fh, "  [AO Login] Waiting for manual login (up to 5 minutes)…", "info")
                        deadline = time.time() + 300
                        while time.time() < deadline:
                            if "nxt.angelone.in" in page.url and "/auth" not in page.url:
                                break
                            time.sleep(2)
                        else:
                            _log(gui, log_fh, "  [AO Login] Timed out waiting for manual login — skipping Phase 5.", "err")
                            gui.stop_progress()
                            gui.set_status("✗ Angel One NXT login timed out.")
                            log_fh.close()
                            return
                        _log(gui, log_fh, "  [AO Login] ✓ Logged in successfully.", "ok")

                _minimize_edge()
                gui.set_status("Phase 5 — Angel One NXT reports…")
                # ── Step 1: Download reports ───────────────────────────────
                _t5 = date.today()
                _stocks_prefix = f"Datawarehouse/Stocks/{_t5.year}/{_t5.month:02d}/{_t5.day:02d}"
                for r in REPORTS:
                    _blob = f"{_stocks_prefix}/{r['gcs_filename']}"
                    if not _force and _gcs_blob_exists("winrich", _blob, gui, log_fh):
                        _log(gui, log_fh,
                             f"\n► [{r['report_name']}] Skipped — already in GCS: {_blob}", "warn")
                    else:
                        gui.set_status(f"Downloading {r['report_name']}…")
                        _download_report(page, r["report_name"], r["csv_name"], gui,
                                         gcs_bucket=r["gcs_bucket"],
                                         gcs_filename=r["gcs_filename"])

                # ── Step 2: Extract customer balances ─────────────────────
                if gui.skip_balances_var.get():
                    _log(gui, log_fh,
                         "\n► Skipping balance extraction (checkbox enabled).", "warn")
                    gui.stop_progress()
                    gui.set_status("✓ Done — balance extraction skipped.")
                    log_fh.close()
                    return

                else:
                    _log(gui, log_fh, "\n► Extracting customer balances…", "info")
                    page.goto(URL_DASHBOARD, wait_until="domcontentloaded", timeout=30_000)
                    page.wait_for_load_state("networkidle", timeout=10_000)

                    today_iso = date.today().isoformat()

                    # Load already-processed customers for today from the checkpoint
                    done_ids: set = set()
                    if gui.resume_var.get() and OUT_CSV.exists():
                        existing = pd.read_csv(OUT_CSV)
                        if "date" in existing.columns and "master_customer_id" in existing.columns:
                            done_ids = set(
                                existing.loc[existing["date"] == today_iso, "master_customer_id"]
                            )
                        if done_ids:
                            _log(gui, log_fh,
                                 f"  [resume] Skipping {len(done_ids)} customers already "
                                 f"processed today.", "warn")

                    results  = []
                    errors   = []
                    today    = today_iso

                    for idx, (_, row) in enumerate(customers.iterrows(), 1):
                        gui.wait_if_paused()

                        master_id = row["master_customer_id"]
                        username  = row["username"]

                        if master_id in done_ids:
                            _log(gui, log_fh,
                                 f"  [{idx}/{total}] {username} — already done today, skipping", "info")
                            continue

                        gui.set_status(f"Extracting balances ({idx}/{total})…  {username}")

                        try:
                            found = _search_customer(page, username)
                            if not found:
                                _log(gui, log_fh,
                                     f"  [{idx}/{total}] {username} — not found, skipped", "warn")
                                results.append({
                                    "date": today,
                                    "master_customer_id": master_id,
                                    "name": username,
                                })
                            else:
                                data     = _extract_customer_data(page)
                                row_data = {
                                    "date": today,
                                    "master_customer_id": master_id,
                                    "name": username,
                                }
                                row_data.update(data)
                                results.append(row_data)

                                margin  = data.get("Margin Available for Trade")
                                balance = data.get("Net Ledger Balance")
                                _log(gui, log_fh,
                                     f"  [{idx}/{total}] {username}  "
                                     f"margin={margin}  balance={balance}", "ok")

                                _save_appended(pd.DataFrame(results))

                            page.goto(URL_DASHBOARD, wait_until="domcontentloaded", timeout=30_000)
                            page.wait_for_load_state("networkidle", timeout=10_000)

                        except Exception as cust_exc:
                            errors.append(username)
                            _log(gui, log_fh,
                                 f"  [{idx}/{total}] {username} — ERROR: {cust_exc}", "err")
                            print(traceback.format_exc(), file=log_fh, flush=True)
                            # Recover: navigate back to dashboard and continue
                            try:
                                page.goto(URL_DASHBOARD, wait_until="domcontentloaded", timeout=30_000)
                                page.wait_for_load_state("networkidle", timeout=10_000)
                            except Exception:
                                pass

                    # ── Final save + GCS upload ────────────────────────────
                    out_df = pd.DataFrame(results)
                    _save_appended(out_df)

                    from agents.gcs_storage_agent import GCSStorageAgent
                    from agents.base import AgentStatus

                    gcs_resp = GCSStorageAgent().run("upload_csv", {
                        "file_path":   str(OUT_CSV),
                        "filename":    "angelone_equity_margins.csv",
                        "bucket_name": "winrich_shared",
                        "prefix":      "data/AngleNxt",
                    })
                    if gcs_resp.status == AgentStatus.SUCCESS:
                        _log(gui, log_fh, f"\n[gcs] ✓ {gcs_resp.output['gcs_uri']}", "ok")
                    else:
                        _log(gui, log_fh, f"\n[gcs] Upload failed: {gcs_resp.error}", "err")

                    captured = out_df["Margin Available for Trade"].notna().sum() if "Margin Available for Trade" in out_df.columns else 0

                    try:
                        email_status = _send_report_email(out_df, total, captured, errors)
                        _log(gui, log_fh, f"[email] ✓ {email_status}", "ok")
                    except Exception as email_exc:
                        _log(gui, log_fh, f"[email] Failed: {email_exc}", "err")

                    gui.stop_progress()
                    summary  = f"✓ All done!  {captured}/{total} captured."
                    if errors:
                        summary += f"  {len(errors)} errors (see log)."
                    gui.set_status(summary)
                    _log(gui, log_fh, f"\n{summary}", "ok")
                    log_fh.close()
                    return

        except Exception as exc:
            gui.stop_progress()
            _log(gui, log_fh, f"\n[error] {exc}", "err")
            print(traceback.format_exc(), file=log_fh, flush=True)

            if not _edge_running():
                gui.set_status("Browser was closed — please log in again…")
                _log(gui, log_fh, "Edge was closed. Relaunching…", "warn")
                time.sleep(2)
                continue

            gui.set_status("Error — see log.")
            log_fh.close()
            return


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    root = tk.Tk()
    gui  = RunnerGUI(root)

    t = threading.Thread(target=_run, args=(gui,), daemon=True)
    t.start()

    root.mainloop()


if __name__ == "__main__":
    main()