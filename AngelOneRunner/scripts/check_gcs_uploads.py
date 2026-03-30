"""
scripts/check_gcs_uploads.py
────────────────────────────
Checks whether all expected daily GCS uploads are present.

On GCP (Cloud Run Job):
  • All present  → logs INFO lines, exits 0
  • Files missing → logs ERROR lines, exits 1
                    → Cloud Monitoring log-based alert fires
                    → notification email includes the actual missing-file log lines

No Gmail app password or SMTP needed.

Expected files
──────────────
  winrich bucket:
    • Unify:   Datawarehouse/Unify/{Y}/{M}/{D}/WAWYA_Daily_AUM_{DD-MM-YYYY}.csv   (yesterday)
    • ASK:     Datawarehouse/ASK/{Y}/{M}/{D}/ask_pms.csv                           (yesterday)
    • Vested:  Datawarehouse/Vested/{Y}/{M}/{D}/funded_users_{YYYY-MM-DD}.csv      (today)
    • IPRU:    Datawarehouse/ICICI-PMS/{Y}/{M}/{D}/ICICIPMS {D}.csv               (today)
    • Stocks:  Datawarehouse/Stocks/{Y}/{M}/{D}/client-dp-holdings.csv             (today)
    • Stocks:  Datawarehouse/Stocks/{Y}/{M}/{D}/Equity.csv                         (today)

  winrich_shared bucket:
    • AngelNxt: data/AngleNxt/angelone_equity_margins.csv                          (static)

Local usage
───────────
  python scripts/check_gcs_uploads.py
  python scripts/check_gcs_uploads.py --date 2026-03-29
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# ── Logging — plain format locally; Cloud Logging picks up severity from level ─
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("check_gcs_uploads")

# ── resolve project root so we can load .env (local runs only) ──────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    _env_path = ROOT / "credentials" / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=True)
except ImportError:
    pass


# ── GCS helpers ──────────────────────────────────────────────────────────────

def _gcs_client():
    """
    Return an authenticated GCS client.

    Local:  uses st.secrets["gcp"] (service account) if available, else ADC.
    GCP:    uses the service account attached to the Cloud Run Job (ADC).
    """
    try:
        from google.cloud import storage  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "google-cloud-storage is not installed — run `pip install google-cloud-storage`"
        ) from exc

    try:
        import streamlit as st  # type: ignore
        creds = st.secrets.get("gcp")
        if creds and (
            creds.get("type") if isinstance(creds, dict) else getattr(creds, "type", None)
        ) == "service_account":
            return storage.Client.from_service_account_info(dict(creds))
    except Exception:
        pass

    return storage.Client()


def _blob_exists(client, bucket_name: str, blob_name: str) -> bool:
    """Return True if blob exists. Errors are treated as missing (returns False)."""
    try:
        return client.bucket(bucket_name).blob(blob_name).exists()
    except Exception as exc:
        log.warning("GCS check error for gs://%s/%s: %s", bucket_name, blob_name, exc)
        return False


# ── Expected blob definitions ─────────────────────────────────────────────────

def _expected_blobs(check_date: date) -> list[dict]:
    """
    All blobs expected to be present for the given check_date.

    Unify and ASK use the previous day's date (they download yesterday's data).
    Everything else uses check_date.
    """
    yesterday = check_date - timedelta(days=1)
    y0, m0, d0 = yesterday.year, yesterday.month, yesterday.day
    y1, m1, d1 = check_date.year, check_date.month, check_date.day

    return [
        {
            "label":  "Unify Daily AUM",
            "bucket": "winrich",
            "blob":   (f"Datawarehouse/Unify/{y0}/{m0:02d}/{d0:02d}"
                       f"/WAWYA_Daily_AUM_{yesterday.strftime('%d-%m-%Y')}.csv"),
        },
        {
            "label":  "ASK PMS",
            "bucket": "winrich",
            "blob":   f"Datawarehouse/ASK/{y0}/{m0:02d}/{d0:02d}/ask_pms.csv",
        },
        {
            "label":  "Vested Funded Users",
            "bucket": "winrich",
            "blob":   (f"Datawarehouse/Vested/{y1}/{m1:02d}/{d1:02d}"
                       f"/funded_users_{check_date.strftime('%Y-%m-%d')}.csv"),
        },
        {
            "label":  "ICICI PMS (IPRU Email)",
            "bucket": "winrich",
            "blob":   f"Datawarehouse/ICICI-PMS/{y1}/{m1:02d}/{d1:02d}/ICICIPMS {d1}.csv",
        },
        {
            "label":  "AngelOne Client DP Holdings",
            "bucket": "winrich",
            "blob":   f"Datawarehouse/Stocks/{y1}/{m1:02d}/{d1:02d}/client-dp-holdings.csv",
        },
        {
            "label":  "AngelOne Equity (Security Holdings)",
            "bucket": "winrich",
            "blob":   f"Datawarehouse/Stocks/{y1}/{m1:02d}/{d1:02d}/Equity.csv",
        },
        {
            "label":  "AngelOne Equity Margins",
            "bucket": "winrich_shared",
            "blob":   "data/AngleNxt/angelone_equity_margins.csv",
        },
    ]


# ── Check logic ───────────────────────────────────────────────────────────────

def check_uploads(check_date: date) -> tuple[list[dict], list[dict]]:
    """Check all expected blobs. Returns (present, missing)."""
    client   = _gcs_client()
    expected = _expected_blobs(check_date)
    present, missing = [], []

    for entry in expected:
        gcs_uri = f"gs://{entry['bucket']}/{entry['blob']}"
        if _blob_exists(client, entry["bucket"], entry["blob"]):
            log.info("OK       %-35s  %s", entry["label"], gcs_uri)
            present.append(entry)
        else:
            log.error("MISSING  %-35s  %s", entry["label"], gcs_uri)
            missing.append(entry)

    return present, missing


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Check daily GCS uploads. Exits 1 if any files are missing."
    )
    parser.add_argument(
        "--date", default=None,
        help="Date to check in YYYY-MM-DD format (default: today)",
    )
    args = parser.parse_args()

    check_date = date.fromisoformat(args.date) if args.date else date.today()

    log.info("GCS upload check — %s", check_date.strftime("%d %b %Y"))

    present, missing = check_uploads(check_date)

    log.info("Result: %d present, %d missing", len(present), len(missing))

    if missing:
        log.error(
            "UPLOAD INCOMPLETE — %d file(s) missing: %s",
            len(missing),
            ", ".join(e["label"] for e in missing),
        )
        sys.exit(1)

    log.info("All files present. No action required.")


if __name__ == "__main__":
    main()
