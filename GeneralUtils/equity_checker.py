#!/usr/bin/env python3
"""
Equity Report Cross-Checker
============================
Parses a Winwize Portfolio Performance Report PDF and cross-checks
its values against the Winwize backend (Frappe on 34.223.1.167).

Checks performed (per pdf_generation_spec.md):
  §02  Portfolio KPIs   — total_invested, cur_val, realised_proceeds, total_pnl
  §04  Holdings table   — qty, buy_amount, current_value, return_pct per stock
  §05  Loss positions   — every loss-making active stock must appear
  §06  Research notes   — every active holding must have an entry in stock_research.csv
  §08  Sector alloc     — industry-wise allocation % (uses tabStocks.industry, not sector)
  §08  Cap alloc        — market_cap_category-wise allocation %
  FILTER               — no stock with holdingqty = 0 should appear in any section

Usage
-----
    python equity_checker.py <pdf_path> [options]

    --customer   WR1001            override customer ID (auto-detected from PDF)
    --output     report.csv        save discrepancy report to CSV
    --research-csv  path/stock_research.csv   check §06 research coverage
    --debug-text                   dump raw PDF text and exit

Environment variables (.env)
-----------------------------
    FRAPPE_URL          http://34.223.1.167:8000     (default)
    FRAPPE_API_KEY      <api_key>
    FRAPPE_API_SECRET   <api_secret>
    # OR
    FRAPPE_USER         Administrator
    FRAPPE_PASS         <password>
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import pdfplumber
import requests
from dotenv import load_dotenv

load_dotenv()

# ── Comparison tolerances ───────────────────────────────────────────────────────
REL_TOL = 0.01   # 1 % relative tolerance
ABS_TOL = 1.00   # ₹1 absolute tolerance (PDF rounds to whole rupees)
ALLOC_TOL = 1.0  # 1 pp allocation tolerance

# ── Frappe connection ────────────────────────────────────────────────────────────
FRAPPE_URL        = os.environ.get("FRAPPE_URL",        "http://34.223.1.167:8000")
FRAPPE_API_KEY    = os.environ.get("FRAPPE_API_KEY",    "")
FRAPPE_API_SECRET = os.environ.get("FRAPPE_API_SECRET", "")
FRAPPE_USER       = os.environ.get("FRAPPE_USER",       "")
FRAPPE_PASS       = os.environ.get("FRAPPE_PASS",       "")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Data structures                                                             ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@dataclass
class PDFHolding:
    """One row from the §04 holdings table in the PDF."""
    nse_code:      str
    qty:           float
    buy_amount:    float
    current_value: float
    pnl:           float
    return_pct:    float


@dataclass
class PDFSummary:
    """Portfolio-level values from the PDF page 1 header (§02 KPIs)."""
    customer_id:        str
    customer_name:      str
    total_invested:     float   # TOTAL INVESTED / TOTAL INVESTMENT
    current_value:      float   # CURRENT VALUE / CURRENT PORTFOLIO VALUE
    realised_proceeds:  float   # REALISED PROCEEDS (total_sell_amt_winwize_orders)
    abs_return_pct:     float   # ABSOLUTE RETURN %


@dataclass
class PDFAllocationEntry:
    category: str
    pct:      float


@dataclass
class PDFAllocation:
    market_cap: List[PDFAllocationEntry]
    sector:     List[PDFAllocationEntry]   # uses industry field, not sector


@dataclass
class AllocationResult:
    category:    str
    pdf_pct:     float
    backend_pct: float
    diff:        float
    severity:    str   # "OK" | "WARNING" | "MISMATCH"


@dataclass
class BackendHolding:
    customer_id:         str
    stock_id:            str
    stock_name:          str
    nse_code:            str
    isin:                str
    qty_held:            float
    average_price:       float
    cmp:                 float
    total_val:           float
    profit_loss_perc:    float
    market_cap_category: str = ""
    sector:              str = ""
    industry:            str = ""   # tabStocks.industry — used for sector allocation
    winwize_qty:           float = 0.0
    winwize_buy_amount:    float = 0.0
    winwize_current_value: float = 0.0
    winwize_return_pct:    float = 0.0


@dataclass
class Discrepancy:
    field:         str
    pdf_value:     float
    backend_value: float
    diff:          float
    diff_pct:      float
    severity:      str    # "OK" | "WARNING" | "MISMATCH"


@dataclass
class HoldingResult:
    nse_code:      str
    stock_name:    str
    status:        str    # "MATCH" | "MISMATCH" | "PDF_ONLY" | "BACKEND_ONLY"
    discrepancies: List[Discrepancy] = field(default_factory=list)


@dataclass
class ResearchCoverage:
    """§06 research coverage per stock."""
    nse_code:   str
    stock_name: str
    covered:    bool   # True if nse_code found in stock_research.csv


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Frappe REST client                                                          ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class FrappeClient:
    def __init__(self) -> None:
        self.base    = FRAPPE_URL.rstrip("/")
        self.session = requests.Session()
        self._auth_header: Dict[str, str] = {}

    def authenticate(self) -> None:
        if FRAPPE_API_KEY and FRAPPE_API_SECRET:
            self._auth_header = {
                "Authorization": f"token {FRAPPE_API_KEY}:{FRAPPE_API_SECRET}"
            }
            return
        if FRAPPE_USER and FRAPPE_PASS:
            resp = self.session.post(
                f"{self.base}/api/method/login",
                data={"usr": FRAPPE_USER, "pwd": FRAPPE_PASS},
                timeout=15,
            )
            resp.raise_for_status()
            return
        raise ValueError(
            "No Frappe credentials found.\n"
            "Set FRAPPE_API_KEY + FRAPPE_API_SECRET  (or FRAPPE_USER + FRAPPE_PASS) in .env"
        )

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        headers = {"Content-Type": "application/json", **self._auth_header}
        url = f"{self.base}{path}"
        try:
            resp = self.session.get(url, params=params, headers=headers, timeout=20)
        except requests.exceptions.ConnectionError:
            raise ConnectionError(
                f"Cannot reach Frappe at {self.base}\n"
                f"  • Is the server running?  ssh ubuntu@34.223.1.167 and check: pm2 status\n"
                f"  • Is port 8000 open?  Try: curl {self.base}/api/method/frappe.ping"
            )
        except requests.exceptions.Timeout:
            raise TimeoutError(f"Request to {url} timed out after 20 s")

        if resp.status_code == 401:
            raise PermissionError(
                "Frappe returned 401 Unauthorized.\n"
                "  • Check FRAPPE_API_KEY / FRAPPE_API_SECRET in .env"
            )
        if resp.status_code == 403:
            raise PermissionError(
                f"Frappe returned 403 Forbidden for {path}\n"
                "  • The user may not have read access to this DocType"
            )
        if resp.status_code == 500:
            raise RuntimeError(
                f"Frappe returned 500 Server Error for {path}\n"
                f"  Response: {resp.text[:300]}"
            )
        resp.raise_for_status()
        return resp.json()

    def ping(self) -> bool:
        try:
            self._get("/api/method/frappe.ping")
            return True
        except Exception:
            return False

    def _get_all_pages(self, path: str, params: dict) -> List[dict]:
        results: List[dict] = []
        page_len = 500
        start    = 0
        while True:
            p    = {**params, "limit_start": start, "limit_page_length": page_len}
            data = self._get(path, p).get("data", [])
            results.extend(data)
            if len(data) < page_len:
                break
            start += page_len
        return results

    def get_customer(self, customer_id: str) -> dict:
        """
        Fetch authoritative customer aggregates from tabCustomers.
        Fields per spec §3: total_buy_amt, cur_val, total_sell_amt_winwize_orders.
        """
        try:
            return self._get(f"/api/resource/Customers/{customer_id}").get("data", {})
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return {}
            raise

    def get_winwize_stocks(self, customer_id: str) -> List[dict]:
        """Fetch all rows from tabCustomer_Winwize_Stocks for this customer."""
        return self._get_all_pages(
            "/api/resource/Customer_Winwize_Stocks",
            {
                "filters": json.dumps([["customerid", "=", customer_id]]),
                "fields": json.dumps([
                    "customerid", "stockid",
                    "holdingqty", "buyqty", "buyamount",
                    "sellqty", "sellamount", "currentvalue",
                    "cmp", "profitpercent",
                ]),
            },
        )

    def get_stock_details(self, stock_ids: List[str]) -> Dict[str, dict]:
        """
        Fetch tabStocks master data.
        Includes `industry` (used for sector allocation, per spec §9)
        and `sector` (informational).
        """
        if not stock_ids:
            return {}
        rows = self._get_all_pages(
            "/api/resource/Stocks",
            {
                "filters": json.dumps([["name", "in", stock_ids]]),
                "fields": json.dumps([
                    "name", "stock_name", "nse_code", "isin_code",
                    "current_price", "market_cap_category", "sector", "industry",
                ]),
            },
        )
        return {r["name"]: r for r in rows}


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  PDF extractor                                                               ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

_HOLDING_RE = re.compile(
    r'^([A-Z][A-Z0-9\-\.&]{1,19})'
    r'\s+(\d+(?:\.\d+)?)'
    r'\s*[·\-\u00b7]\s*'
    r'Rs\s*([\d,]+(?:\.\d+)?)'
    r'\s+Rs\s*([\d,]+(?:\.\d+)?)'
    r'\s+Rs\s*(-?[\d,]+(?:\.\d+)?)'
    r'\s+([+\-][\d.]+)%?',
    re.MULTILINE,
)

_QTY_BUY_RE = re.compile(
    r'(\d+(?:\.\d+)?)\s*[·\-\u00b7]\s*Rs\s*([\d,]+(?:\.\d+)?)'
)


class WinwizePDFExtractor:
    def __init__(self, pdf_path: str) -> None:
        self.pdf_path = pdf_path

    def extract(self) -> Tuple[PDFSummary, List[PDFHolding], PDFAllocation]:
        page_texts: List[str] = []
        all_tables: List[List[List]] = []

        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                page_texts.append(text)
                for tbl in (page.extract_tables() or []):
                    all_tables.append(tbl)

        full_text  = "\n".join(page_texts)
        page1_text = page_texts[0] if page_texts else full_text
        page2_text = page_texts[1] if len(page_texts) > 1 else full_text

        summary    = self._extract_summary(page1_text)
        allocation = self._extract_allocations(page2_text)
        holdings   = self._extract_from_tables(all_tables)
        if not holdings:
            holdings = self._extract_from_text(full_text)

        # Deduplicate on NSE code
        seen: Dict[str, PDFHolding] = {}
        for h in holdings:
            if h.nse_code and h.nse_code not in seen:
                seen[h.nse_code] = h

        return summary, list(seen.values()), allocation

    def _extract_summary(self, text: str) -> PDFSummary:
        # Customer ID
        cid = ""
        m = re.search(r'\bWR\d+\b', text, re.IGNORECASE)
        if m:
            cid = m.group(0).upper()

        # Customer name (word(s) before WR\d+)
        name = ""
        m = re.search(r'^(.+?)\s+WR\d+', text, re.MULTILINE)
        if m:
            raw = m.group(1).strip()
            name = re.sub(r'[\s·\-\|]+$', '', raw).strip()
            if name == name.upper() and len(name) > 4:
                name = ""

        # KPI values — each box may have label and value on separate lines.
        # Strategy: for each KPI label, grab the first Rs amount within the
        # next 2 lines after that label. This handles both single-line and
        # multi-line layouts.
        total_invested    = 0.0
        current_value     = 0.0
        realised_proceeds = 0.0
        abs_return        = 0.0

        def _first_rs_after(pattern: str, txt: str) -> float:
            """Find first Rs amount within 2 lines after a label pattern."""
            m = re.search(pattern, txt, re.IGNORECASE)
            if not m:
                return 0.0
            snippet = txt[m.end(): m.end() + 200]
            rm = re.search(r'Rs\s*([\d,]+(?:\.\d+)?)', snippet)
            return _parse_num(rm.group(1)) if rm else 0.0

        total_invested    = _first_rs_after(r'TOTAL\s+INVEST\w*', text)
        current_value     = _first_rs_after(r'CURRENT\s+(?:PORTFOLIO\s+)?VALUE', text)
        realised_proceeds = _first_rs_after(r'REALISED?\s+PROCEEDS?', text)

        # Return %
        pct_m = re.search(r'ABSOLUTE\s+RETURN[^\n]*?([+\-][\d.]+)%', text, re.IGNORECASE)
        if not pct_m:
            pct_m = re.search(r'TOTAL\s+P&?L[^\n]*?([+\-][\d.]+)%', text, re.IGNORECASE)
        if pct_m:
            try:
                abs_return = float(pct_m.group(1))
            except ValueError:
                pass

        return PDFSummary(
            customer_id=cid,
            customer_name=name,
            total_invested=total_invested,
            current_value=current_value,
            realised_proceeds=realised_proceeds,
            abs_return_pct=abs_return,
        )

    def _extract_from_tables(self, tables: List[List[List]]) -> List[PDFHolding]:
        holdings: List[PDFHolding] = []
        for tbl in tables:
            rows = self._parse_winwize_table(tbl)
            holdings.extend(rows)
        return holdings

    def _is_holdings_table(self, row: List) -> bool:
        text = " ".join(str(c or "").lower() for c in row)
        return "stock" in text and ("qty" in text or "buy" in text or "value" in text)

    def _parse_winwize_table(self, table: List[List]) -> List[PDFHolding]:
        if not table or len(table) < 2:
            return []
        header_idx = None
        for i, row in enumerate(table):
            if self._is_holdings_table(row):
                header_idx = i
                break
        if header_idx is None:
            return []

        headers = [str(c or "").strip().lower() for c in table[header_idx]]
        col_stock = col_qty_buy = col_qty = col_buy = col_curval = col_pnl = col_return = None

        for i, h in enumerate(headers):
            if "stock" in h and col_stock is None:
                col_stock = i
            elif "qty" in h and "buy" in h:
                col_qty_buy = i
            elif re.search(r'\bqty\b|\bquantity\b', h) and col_qty is None:
                col_qty = i
            elif "buy" in h and col_buy is None:
                col_buy = i
            elif re.search(r'current\s*value|cur.*val', h) and col_curval is None:
                col_curval = i
            elif re.search(r'p&l|pnl|unrl', h) and col_pnl is None:
                col_pnl = i
            elif "return" in h and col_return is None:
                col_return = i

        if col_stock is None or col_curval is None:
            return []

        holdings: List[PDFHolding] = []
        for row in table[header_idx + 1:]:
            if not row or all(c is None or str(c).strip() == "" for c in row):
                continue
            nse = str(row[col_stock] or "").strip().upper()
            if not nse or any(kw in nse.lower() for kw in ["total", "grand", "top"]):
                continue
            if not re.fullmatch(r'[A-Z][A-Z0-9\-\.&]{1,19}', nse):
                continue

            qty = buy = 0.0
            if col_qty_buy is not None:
                cell = str(row[col_qty_buy] or "")
                m = _QTY_BUY_RE.search(cell)
                if m:
                    qty = float(m.group(1))
                    buy = _parse_num(m.group(2))
            else:
                if col_qty is not None:
                    qty = _parse_num(str(row[col_qty] or ""))
                if col_buy is not None:
                    buy = _parse_num(str(row[col_buy] or ""))

            curval = _parse_num(str(row[col_curval] or "")) if col_curval is not None else 0.0
            pnl    = _parse_num(str(row[col_pnl]    or "")) if col_pnl    is not None else 0.0
            ret    = _parse_num(str(row[col_return]  or "")) if col_return  is not None else 0.0

            holdings.append(PDFHolding(
                nse_code=nse, qty=qty, buy_amount=buy,
                current_value=curval, pnl=pnl, return_pct=ret,
            ))
        return holdings

    def _extract_from_text(self, text: str) -> List[PDFHolding]:
        holdings: List[PDFHolding] = []
        normalised = text.replace("\u00b7", "·").replace(" - ", " · ")
        for m in _HOLDING_RE.finditer(normalised):
            holdings.append(PDFHolding(
                nse_code=m.group(1).strip().upper(),
                qty=float(m.group(2)),
                buy_amount=_parse_num(m.group(3)),
                current_value=_parse_num(m.group(4)),
                pnl=_parse_num(m.group(5)),
                return_pct=float(m.group(6)),
            ))
        return holdings

    _ALLOC_ENTRY_RE = re.compile(
        r'([A-Za-z][A-Za-z\-&]*(?:\s+[A-Za-z][A-Za-z\-&]*)*)\s+([\d]+(?:\.\d+)?)%'
    )

    def _extract_allocations(self, text: str) -> PDFAllocation:
        m = re.search(
            r'(?:SECTION\s+0*2|MARKET\s+CAP\s+ALLOC|SECTORAL?\s+ALLOC)'
            r'(.+?)(?:SECTION\s+0*3|HOLDINGS\s+TABLE|\Z)',
            text, re.IGNORECASE | re.DOTALL,
        )
        block = m.group(1) if m else text

        market_cap: List[PDFAllocationEntry] = []
        sector:     List[PDFAllocationEntry] = []

        for em in self._ALLOC_ENTRY_RE.finditer(block):
            name = em.group(1).strip()
            pct  = float(em.group(2))
            if re.search(r'\bsection\b|\balloc\b|\bportfolio\b', name, re.IGNORECASE):
                continue
            if re.search(r'\bcap\b', name, re.IGNORECASE):
                market_cap.append(PDFAllocationEntry(category=name, pct=pct))
            else:
                sector.append(PDFAllocationEntry(category=name, pct=pct))

        return PDFAllocation(market_cap=market_cap, sector=sector)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Research CSV checker (§06)                                                  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def load_research_csv(csv_path: str) -> Set[str]:
    """Return the set of upper-cased NSE codes that have research notes in the CSV."""
    codes: Set[str] = set()
    if not csv_path or not os.path.isfile(csv_path):
        return codes
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("nse_code") or "").strip().upper()
            note = (row.get("research_note") or "").strip()
            if code and note:
                codes.add(code)
    return codes


def check_research_coverage(
    backend_holdings: List[BackendHolding],
    research_codes: Set[str],
) -> List[ResearchCoverage]:
    """
    Per spec §7: every active holding (qty > 0) should have a research note.
    Returns one ResearchCoverage per stock, flagging those that are missing.
    """
    results: List[ResearchCoverage] = []
    for h in sorted(backend_holdings, key=lambda x: x.nse_code):
        covered = h.nse_code.upper() in research_codes
        results.append(ResearchCoverage(
            nse_code=h.nse_code,
            stock_name=h.stock_name,
            covered=covered,
        ))
    return results


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Utility                                                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def _parse_num(s: str) -> float:
    if not s:
        return 0.0
    cleaned = re.sub(r'[Rs₹,\s%]', '', str(s).strip())
    if cleaned.startswith('(') and cleaned.endswith(')'):
        try:
            return -float(cleaned[1:-1])
        except ValueError:
            return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Comparator                                                                  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def _near(a: float, b: float) -> bool:
    if b == 0 and a == 0:
        return True
    if b == 0:
        return abs(a) <= ABS_TOL
    return abs(a - b) / abs(b) <= REL_TOL or abs(a - b) <= ABS_TOL


def _disc(field_name: str, pdf_val: float, be_val: float) -> Discrepancy:
    diff     = pdf_val - be_val
    diff_pct = (diff / abs(be_val) * 100) if be_val != 0 else float("inf")
    ok       = _near(pdf_val, be_val)
    severity = "OK" if ok else ("WARNING" if abs(diff_pct) < 5 else "MISMATCH")
    return Discrepancy(
        field=field_name, pdf_value=pdf_val, backend_value=be_val,
        diff=diff, diff_pct=diff_pct, severity=severity,
    )


class Comparator:
    def __init__(
        self,
        pdf_holdings:     List[PDFHolding],
        backend_holdings: List[BackendHolding],
    ) -> None:
        self.pdf     = {h.nse_code.upper(): h for h in pdf_holdings     if h.nse_code}
        self.backend = {h.nse_code.upper(): h for h in backend_holdings if h.nse_code}

    def compare(self) -> List[HoldingResult]:
        results: List[HoldingResult] = []
        for nse in sorted(set(self.pdf) | set(self.backend)):
            pdf_h = self.pdf.get(nse)
            be_h  = self.backend.get(nse)

            if pdf_h and not be_h:
                results.append(HoldingResult(nse_code=nse, stock_name=nse, status="PDF_ONLY"))
                continue
            if be_h and not pdf_h:
                results.append(HoldingResult(
                    nse_code=nse, stock_name=be_h.stock_name, status="BACKEND_ONLY"
                ))
                continue

            discs: List[Discrepancy] = []

            # qty: PDF qty vs tabCustomer_Winwize_Stocks.holdingqty
            if pdf_h.qty != 0 or be_h.winwize_qty != 0:
                discs.append(_disc("qty", pdf_h.qty, be_h.winwize_qty))

            # buy_amount: PDF vs tabCustomer_Winwize_Stocks.buyamount
            if pdf_h.buy_amount != 0 or be_h.winwize_buy_amount != 0:
                discs.append(_disc("buy_amount", pdf_h.buy_amount, be_h.winwize_buy_amount))

            # current_value: PDF vs tabCustomer_Winwize_Stocks.currentvalue
            if pdf_h.current_value != 0 or be_h.winwize_current_value != 0:
                discs.append(_disc("current_value", pdf_h.current_value, be_h.winwize_current_value))

            # return_pct: PDF vs tabCustomer_Winwize_Stocks.profitpercent
            if pdf_h.return_pct != 0 or be_h.winwize_return_pct != 0:
                discs.append(_disc("return_pct", pdf_h.return_pct, be_h.winwize_return_pct))

            overall = "MATCH" if all(d.severity == "OK" for d in discs) else "MISMATCH"
            results.append(HoldingResult(
                nse_code=nse,
                stock_name=be_h.stock_name or nse,
                status=overall,
                discrepancies=discs,
            ))
        return results

    def summary_check(
        self,
        summary:       PDFSummary,
        customer_info: dict,
        pdf_holdings:  List[PDFHolding],
    ) -> List[Discrepancy]:
        """
        §02 KPI cross-check per spec §3:
          total_invested          →  tabCustomers.total_buy_amt
          current_value           →  tabCustomers.cur_val
          realised_proceeds       →  tabCustomers.total_sell_amt_winwize_orders
          total_pnl               →  cur_val + realised - total_buy_amt  (computed)
          total_stocks (row count)→  tabCustomers.total_stocks
        """
        discs: List[Discrepancy] = []

        be_total_buy  = float(customer_info.get("total_buy_amt",                   0) or 0)
        be_cur_val    = float(customer_info.get("cur_val",                         0) or 0)
        be_realised   = float(customer_info.get("total_sell_amt_winwize_orders",   0) or 0)
        be_total_stk  = float(customer_info.get("total_stocks",                    0) or 0)
        be_total_pnl  = be_cur_val + be_realised - be_total_buy

        # PDF total_pnl: infer from PDF values
        pdf_total_pnl = summary.current_value + summary.realised_proceeds - summary.total_invested

        if summary.total_invested != 0 or be_total_buy != 0:
            discs.append(_disc("total_invested", summary.total_invested, be_total_buy))
        if summary.current_value != 0 or be_cur_val != 0:
            discs.append(_disc("cur_val", summary.current_value, be_cur_val))
        if summary.realised_proceeds != 0 or be_realised != 0:
            discs.append(_disc("realised_proceeds", summary.realised_proceeds, be_realised))
        if pdf_total_pnl != 0 or be_total_pnl != 0:
            discs.append(_disc("total_pnl", pdf_total_pnl, be_total_pnl))
        if len(pdf_holdings) != 0 or be_total_stk != 0:
            discs.append(_disc("total_stocks", float(len(pdf_holdings)), be_total_stk))

        return discs


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  §05 Loss position checker                                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def check_loss_positions(
    pdf_holdings: List[PDFHolding],
    backend_holdings: List[BackendHolding],
) -> Tuple[List[str], List[str]]:
    """
    Per spec §6:
    §05 should contain every active stock where profitpercent < 0.
    Returns (missing_from_pdf, extra_in_pdf) lists of NSE codes.
    """
    pdf_loss_codes  = {h.nse_code.upper() for h in pdf_holdings if h.return_pct < 0}
    db_loss_codes   = {h.nse_code.upper() for h in backend_holdings if h.winwize_return_pct < 0}

    missing = sorted(db_loss_codes - pdf_loss_codes)   # DB says loss, PDF doesn't show
    extra   = sorted(pdf_loss_codes - db_loss_codes)   # PDF says loss, DB doesn't agree
    return missing, extra


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Allocation helpers                                                          ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def compute_backend_allocations(holdings: List[BackendHolding]) -> PDFAllocation:
    """
    Per spec §9/§11: sector allocation uses tabStocks.industry (not sector).
    Cap allocation uses tabStocks.market_cap_category.
    Weight = current_value (winwize_current_value).
    """
    total = sum(h.winwize_current_value for h in holdings if h.winwize_current_value > 0)
    if total == 0:
        return PDFAllocation(market_cap=[], sector=[])

    mktcap_sums: Dict[str, float] = {}
    industry_sums: Dict[str, float] = {}
    for h in holdings:
        val = h.winwize_current_value
        if val <= 0:
            continue
        cap = (h.market_cap_category or "Others").strip() or "Others"
        ind = (h.industry            or "Others").strip() or "Others"
        mktcap_sums[cap]    = mktcap_sums.get(cap, 0.0)    + val
        industry_sums[ind]  = industry_sums.get(ind, 0.0)  + val

    market_cap = [
        PDFAllocationEntry(category=cat, pct=round(val / total * 100, 1))
        for cat, val in sorted(mktcap_sums.items(), key=lambda kv: -kv[1])
    ]
    sector = [
        PDFAllocationEntry(category=cat, pct=round(val / total * 100, 1))
        for cat, val in sorted(industry_sums.items(), key=lambda kv: -kv[1])
    ]
    return PDFAllocation(market_cap=market_cap, sector=sector)


def _norm_cat(s: str) -> str:
    return re.sub(r'\s+', ' ', s.strip().lower())


def compare_allocations(
    pdf_alloc:     PDFAllocation,
    backend_alloc: PDFAllocation,
) -> Tuple[List[AllocationResult], List[AllocationResult]]:
    def _cmp(
        pdf_entries: List[PDFAllocationEntry],
        be_entries:  List[PDFAllocationEntry],
    ) -> List[AllocationResult]:
        pdf_map = {_norm_cat(e.category): e for e in pdf_entries}
        be_map  = {_norm_cat(e.category): e for e in be_entries}
        results: List[AllocationResult] = []
        for key in sorted(set(pdf_map) | set(be_map)):
            pdf_pct = pdf_map[key].pct if key in pdf_map else 0.0
            be_pct  = be_map[key].pct  if key in be_map  else 0.0
            diff    = pdf_pct - be_pct
            cat     = (pdf_map.get(key) or be_map[key]).category
            severity = "OK" if abs(diff) <= ALLOC_TOL else ("WARNING" if abs(diff) <= 5.0 else "MISMATCH")
            results.append(AllocationResult(
                category=cat, pdf_pct=pdf_pct, backend_pct=be_pct, diff=diff, severity=severity,
            ))
        return results

    return (
        _cmp(pdf_alloc.market_cap, backend_alloc.market_cap),
        _cmp(pdf_alloc.sector,     backend_alloc.sector),
    )


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Table renderer                                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def _tbl(headers: List[str], rows: List[List[str]], aligns: str = "") -> str:
    cols = len(headers)
    if not aligns:
        aligns = "l" + "r" * (cols - 1)
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    def fmt(cell: str, i: int) -> str:
        w = widths[i]
        a = aligns[i] if i < len(aligns) else "l"
        if a == "r": return str(cell).rjust(w)
        if a == "c": return str(cell).center(w)
        return str(cell).ljust(w)

    sep = "┼".join("─" * (w + 2) for w in widths)
    top = "┬".join("─" * (w + 2) for w in widths)
    bot = "┴".join("─" * (w + 2) for w in widths)
    lines = [
        "┌" + top + "┐",
        "│" + "│".join(f" {fmt(h, i)} " for i, h in enumerate(headers)) + "│",
        "├" + sep + "┤",
    ]
    for row in rows:
        lines.append("│" + "│".join(f" {fmt(str(row[i]), i)} " for i in range(cols)) + "│")
    lines.append("└" + bot + "┘")
    return "\n".join(lines)


def _sev(severity: str) -> str:
    return {"OK": "✓ OK", "WARNING": "~ WARN", "MISMATCH": "✗ MISMATCH"}.get(severity, severity)


def _status(status: str) -> str:
    return {
        "MATCH":        "✓ MATCH",
        "MISMATCH":     "✗ MISMATCH",
        "PDF_ONLY":     "? PDF only",
        "BACKEND_ONLY": "! DB only",
    }.get(status, status)


def _fmt_num(v: float, is_pct: bool = False) -> str:
    if is_pct:
        return f"{v:+.2f}%"
    return f"{v:,.2f}" if v != int(v) else f"{int(v):,}"


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Report                                                                      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def print_report(
    summary:          PDFSummary,
    customer_info:    dict,
    results:          List[HoldingResult],
    summary_discs:    List[Discrepancy],
    pdf_map:          Dict[str, PDFHolding],
    be_map:           Dict[str, BackendHolding],
    mktcap_results:   Optional[List[AllocationResult]] = None,
    sector_results:   Optional[List[AllocationResult]] = None,
    loss_missing:     Optional[List[str]] = None,
    loss_extra:       Optional[List[str]] = None,
    research_results: Optional[List[ResearchCoverage]] = None,
    output_csv:       Optional[str] = None,
) -> None:
    total    = len(results)
    match    = sum(1 for r in results if r.status == "MATCH")
    mismatch = sum(1 for r in results if r.status == "MISMATCH")
    pdf_only = sum(1 for r in results if r.status == "PDF_ONLY")
    be_only  = sum(1 for r in results if r.status == "BACKEND_ONLY")

    kpi_errors = sum(1 for d in summary_discs if d.severity != "OK")
    loss_errors = len(loss_missing or []) + len(loss_extra or [])
    research_missing = [r for r in (research_results or []) if not r.covered]

    # ── Header ───────────────────────────────────────────────────────────────
    print(f"\n{'═'*80}")
    print(f"  WINWIZE EQUITY REPORT CHECKER  ·  {summary.customer_id}")
    print(f"  PDF : {summary.customer_name}")
    if customer_info:
        print(f"  DB  : {customer_info.get('customer_name','—')}  │  "
              f"Risk: {customer_info.get('risk_profile','—')}  │  "
              f"Status: {customer_info.get('customer_status','—')}")
    print(f"  ─────────────────────────────────────────────────────────")
    print(f"  §02 KPI errors       : {kpi_errors}")
    print(f"  §04 Holdings  → Total: {total}  Match: {match}  "
          f"Mismatch: {mismatch}  PDF-only: {pdf_only}  DB-only: {be_only}")
    print(f"  §05 Loss pos errors  : {loss_errors}")
    if research_results is not None:
        print(f"  §06 Research missing : {len(research_missing)} / {len(research_results)}")
    print(f"{'═'*80}\n")

    # ── Table 1: §02 Portfolio KPI summary ───────────────────────────────────
    print("  §02  PORTFOLIO KPIs  (PDF header  vs  tabCustomers)\n")
    sum_rows = []
    for d in summary_discs:
        is_pct = "return" in d.field or "pct" in d.field
        sum_rows.append([
            d.field,
            _fmt_num(d.pdf_value, is_pct),
            _fmt_num(d.backend_value, is_pct),
            _fmt_num(d.diff, is_pct),
            _sev(d.severity),
        ])
    print("  " + _tbl(
        ["Field", "PDF Value", "DB Value", "Diff", "Result"],
        sum_rows, aligns="lrrrl",
    ).replace("\n", "\n  "))

    # ── Table 2: §04 Holdings comparison ─────────────────────────────────────
    print("\n  §04  HOLDINGS  (PDF  vs  tabCustomer_Winwize_Stocks)\n")
    hold_rows = []
    for r in sorted(results, key=lambda x: (x.status not in ("MISMATCH","PDF_ONLY","BACKEND_ONLY"), x.nse_code)):
        ph = pdf_map.get(r.nse_code.upper())
        bh = be_map.get(r.nse_code.upper())

        if r.status == "PDF_ONLY":
            hold_rows.append([r.nse_code,
                _fmt_num(ph.qty) if ph else "—", "—",
                _fmt_num(ph.buy_amount) if ph else "—", "—",
                _fmt_num(ph.current_value) if ph else "—", "—",
                _fmt_num(ph.return_pct, True) if ph else "—", "—",
                "? PDF only"])
        elif r.status == "BACKEND_ONLY":
            hold_rows.append([r.nse_code,
                "—", _fmt_num(bh.winwize_qty) if bh else "—",
                "—", _fmt_num(bh.winwize_buy_amount) if bh else "—",
                "—", _fmt_num(bh.winwize_current_value) if bh else "—",
                "—", _fmt_num(bh.winwize_return_pct, True) if bh else "—",
                "! DB only"])
        else:
            def cell(fname: str, pv: float, dv: float, is_pct: bool = False) -> Tuple[str, str]:
                d = next((d for d in r.discrepancies if d.field == fname), None)
                flag = "" if (d is None or d.severity == "OK") else " ✗"
                return _fmt_num(pv, is_pct) + flag, _fmt_num(dv, is_pct) + flag

            qty_p,  qty_b  = cell("qty",          ph.qty,           bh.winwize_qty)
            buy_p,  buy_b  = cell("buy_amount",    ph.buy_amount,    bh.winwize_buy_amount)
            val_p,  val_b  = cell("current_value", ph.current_value, bh.winwize_current_value)
            ret_p,  ret_b  = cell("return_pct",    ph.return_pct,    bh.winwize_return_pct, True)
            hold_rows.append([r.nse_code,
                qty_p, qty_b, buy_p, buy_b, val_p, val_b, ret_p, ret_b,
                _status(r.status)])

    print("  " + _tbl(
        ["Stock", "PDF Qty", "DB Qty", "PDF Buy ₹", "DB Buy ₹",
         "PDF Val ₹", "DB Val ₹", "PDF Ret%", "DB Ret%", "Result"],
        hold_rows, aligns="lrrrrrrrrrl",
    ).replace("\n", "\n  "))

    # ── §05 Loss positions check ──────────────────────────────────────────────
    print("\n  §05  LOSS POSITIONS CHECK\n")
    if not loss_missing and not loss_extra:
        print("  ✓ All loss positions match between PDF and DB.\n")
    else:
        if loss_missing:
            print(f"  ✗ MISSING from PDF (DB shows loss, not in PDF §05):")
            for code in loss_missing:
                bh = be_map.get(code)
                ret = f"{bh.winwize_return_pct:+.2f}%" if bh else "?"
                print(f"       {code:<20}  DB return: {ret}")
        if loss_extra:
            print(f"\n  ~ IN PDF but DB return is positive (may have recovered):")
            for code in loss_extra:
                ph = pdf_map.get(code)
                ret = f"{ph.return_pct:+.2f}%" if ph else "?"
                print(f"       {code:<20}  PDF return: {ret}")
        print()

    # ── §06 Research coverage ─────────────────────────────────────────────────
    if research_results is not None:
        print("  §06  STOCK RESEARCH COVERAGE\n")
        if not research_missing:
            print(f"  ✓ All {len(research_results)} active holdings have research notes.\n")
        else:
            print(f"  ✗ {len(research_missing)} holdings missing research notes (will show 'Stock research not found'):\n")
            rows = [[r.nse_code, r.stock_name] for r in research_missing]
            print("  " + _tbl(["NSE Code", "Stock Name"], rows, aligns="ll").replace("\n", "\n  "))
            print()

    # ── §08 Allocation checks ─────────────────────────────────────────────────
    def _alloc_table(title: str, alloc_results: List[AllocationResult]) -> None:
        print(f"\n  §08  {title}  (PDF  vs  DB tabStocks)\n")
        if not alloc_results:
            print("  (no allocation data available)\n")
            return
        rows = [
            [r.category, f"{r.pdf_pct:.1f}%", f"{r.backend_pct:.1f}%",
             f"{r.diff:+.1f}%", _sev(r.severity)]
            for r in sorted(alloc_results, key=lambda x: -x.pdf_pct)
        ]
        print("  " + _tbl(
            ["Category", "PDF %", "DB %", "Diff", "Result"],
            rows, aligns="lrrrl",
        ).replace("\n", "\n  "))

    if mktcap_results is not None:
        _alloc_table("MARKET CAP ALLOCATION", mktcap_results)
    if sector_results is not None:
        _alloc_table("INDUSTRY / SECTOR ALLOCATION", sector_results)

    # ── CSV output ────────────────────────────────────────────────────────────
    if output_csv:
        _write_csv(
            summary.customer_id, results, summary_discs,
            loss_missing or [], loss_extra or [],
            research_results or [], output_csv,
        )
        print(f"\n  Discrepancy report saved → {output_csv}")


def _write_csv(
    customer_id:      str,
    results:          List[HoldingResult],
    summary_discs:    List[Discrepancy],
    loss_missing:     List[str],
    loss_extra:       List[str],
    research_results: List[ResearchCoverage],
    path:             str,
) -> None:
    COLS = [
        "customer_id", "section", "nse_code", "stock_name",
        "check", "pdf_value", "backend_value", "diff", "diff_pct", "severity",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(COLS)

        # Sec02 KPI rows
        for d in summary_discs:
            w.writerow([customer_id, "Sec02_kpi", "", "",
                d.field, d.pdf_value, d.backend_value, d.diff,
                f"{d.diff_pct:.4f}" if d.diff_pct != float("inf") else "inf",
                d.severity])

        # Sec04 holdings rows
        for r in results:
            if not r.discrepancies:
                w.writerow([customer_id, "Sec04_holdings", r.nse_code, r.stock_name,
                    r.status, "", "", "", "", r.status])
            else:
                for d in r.discrepancies:
                    w.writerow([customer_id, "Sec04_holdings", r.nse_code, r.stock_name,
                        d.field, d.pdf_value, d.backend_value, d.diff,
                        f"{d.diff_pct:.4f}" if d.diff_pct != float("inf") else "inf",
                        d.severity])

        # Sec05 loss position rows
        for code in loss_missing:
            w.writerow([customer_id, "Sec05_loss", code, "", "missing_from_pdf", "", "", "", "", "MISMATCH"])
        for code in loss_extra:
            w.writerow([customer_id, "Sec05_loss", code, "", "extra_in_pdf", "", "", "", "", "WARNING"])

        # Sec06 research rows
        for r in research_results:
            if not r.covered:
                w.writerow([customer_id, "Sec06_research", r.nse_code, r.stock_name,
                    "no_research_note", "", "", "", "", "WARNING"])


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Main                                                                        ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cross-check a Winwize Equity Report PDF against the backend DB"
    )
    parser.add_argument("pdf", help="Path to Winwize Portfolio Performance Report PDF")
    parser.add_argument("--customer", "-c",
                        help="Winwize customer ID (e.g. WR1001). Auto-detected from PDF if omitted.")
    parser.add_argument("--output", "-o",
                        help="Save discrepancy report to this CSV file")
    parser.add_argument("--research-csv", "-r",
                        help="Path to stock_research.csv for §06 coverage check")
    parser.add_argument("--debug-text", action="store_true",
                        help="Dump raw PDF text per page and exit (diagnose extraction issues)")
    args = parser.parse_args()

    if args.debug_text:
        with pdfplumber.open(args.pdf) as _pdf:
            for i, _page in enumerate(_pdf.pages, 1):
                print(f"\n{'─'*60}  PAGE {i}  {'─'*60}\n")
                print(_page.extract_text() or "(no text)")
        sys.exit(0)

    if not os.path.isfile(args.pdf):
        print(f"ERROR: PDF not found: {args.pdf}", file=sys.stderr)
        sys.exit(1)

    # ── Step 1: extract PDF ──────────────────────────────────────────────────
    print("[1/3] Extracting data from PDF …")
    extractor = WinwizePDFExtractor(args.pdf)
    summary, pdf_holdings, pdf_alloc = extractor.extract()

    customer_id = args.customer or summary.customer_id
    if not customer_id:
        print("ERROR: Could not detect customer ID.\n       Pass it with --customer WR1001",
              file=sys.stderr)
        sys.exit(1)

    # Limit §04/§05 comparison to top 10 holdings by current value (as shown in PDF)
    pdf_holdings_all = pdf_holdings
    pdf_holdings = sorted(pdf_holdings, key=lambda h: h.current_value, reverse=True)[:10]

    print(f"       Customer         : {customer_id}  ({summary.customer_name})")
    print(f"       Total invested   : ₹{summary.total_invested:,.0f}")
    print(f"       Current value    : ₹{summary.current_value:,.0f}")
    print(f"       Realised proceeds: ₹{summary.realised_proceeds:,.0f}")
    print(f"       Holdings in PDF  : {len(pdf_holdings_all)} extracted, comparing top {len(pdf_holdings)}")

    if not pdf_holdings:
        print("ERROR: No holdings extracted from PDF.\n"
              "       Ensure the PDF is a Winwize Portfolio Performance Report.\n"
              "       Try --debug-text to inspect raw PDF content.", file=sys.stderr)
        sys.exit(1)

    # ── Step 2: fetch Winwize backend ────────────────────────────────────────
    print(f"[2/3] Fetching backend data from {FRAPPE_URL} …")
    client = FrappeClient()
    try:
        client.authenticate()
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        customer_info = client.get_customer(customer_id)
        if not customer_info:
            print(f"WARNING: Customer '{customer_id}' not found in backend.", file=sys.stderr)

        ww_rows   = client.get_winwize_stocks(customer_id)
        # Per spec §2 filter: only active holdings (holdingqty > 0)
        active_ww = [r for r in ww_rows if float(r.get("holdingqty", 0) or 0) > 0]

        stock_ids    = [r["stockid"] for r in active_ww if r.get("stockid")]
        stock_master = client.get_stock_details(stock_ids)

    except (ConnectionError, TimeoutError, PermissionError, RuntimeError) as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except requests.HTTPError as e:
        print(f"\nERROR: HTTP {e.response.status_code} for {e.request.url}\n  {e.response.text[:300]}",
              file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Unexpected error: {e}", file=sys.stderr)
        raise

    backend_holdings: List[BackendHolding] = []
    for ww in active_ww:
        sid = ww.get("stockid", "")
        sm  = stock_master.get(sid, {})
        wz_qty = float(ww.get("holdingqty")    or 0)
        wz_buy = float(ww.get("buyamount")     or 0)
        wz_val = float(ww.get("currentvalue")  or 0)
        wz_ret = float(ww.get("profitpercent") or 0)
        backend_holdings.append(BackendHolding(
            customer_id=customer_id,
            stock_id=sid,
            stock_name=sm.get("stock_name", sid),
            nse_code=sid,
            isin=sm.get("isin_code", ""),
            qty_held=wz_qty,
            average_price=float(ww.get("cmp") or 0),
            cmp=float(ww.get("cmp") or 0),
            total_val=wz_val,
            profit_loss_perc=wz_ret,
            market_cap_category=(sm.get("market_cap_category") or "Others").strip(),
            sector=(sm.get("sector")   or "").strip(),
            industry=(sm.get("industry") or "Others").strip(),
            winwize_qty=wz_qty,
            winwize_buy_amount=wz_buy,
            winwize_current_value=wz_val,
            winwize_return_pct=wz_ret,
        ))

    print(f"       DB active stocks : {len(active_ww)}  "
          f"({len(ww_rows) - len(active_ww)} sold/inactive skipped)")

    # ── Step 3: compare ──────────────────────────────────────────────────────
    print("[3/3] Comparing …\n")
    comparator    = Comparator(pdf_holdings, backend_holdings)
    results       = comparator.compare()
    summary_discs = comparator.summary_check(summary, customer_info, pdf_holdings)

    pdf_map = {h.nse_code.upper(): h for h in pdf_holdings     if h.nse_code}
    be_map  = {h.nse_code.upper(): h for h in backend_holdings if h.nse_code}

    backend_alloc               = compute_backend_allocations(backend_holdings)
    mktcap_results, sec_results = compare_allocations(pdf_alloc, backend_alloc)

    # §05 loss position check
    loss_missing, loss_extra = check_loss_positions(pdf_holdings, backend_holdings)

    # §06 research coverage check
    research_results: Optional[List[ResearchCoverage]] = None
    if args.research_csv:
        research_codes   = load_research_csv(args.research_csv)
        research_results = check_research_coverage(backend_holdings, research_codes)
    else:
        print("       (§06 research check skipped — pass --research-csv to enable)")

    print(f"       PDF allocations  : "
          f"{len(pdf_alloc.market_cap)} cap, {len(pdf_alloc.sector)} sector")
    print(f"       DB  allocations  : "
          f"{len(backend_alloc.market_cap)} cap, {len(backend_alloc.sector)} sector\n")

    print_report(
        summary, customer_info, results, summary_discs,
        pdf_map=pdf_map, be_map=be_map,
        mktcap_results=mktcap_results,
        sector_results=sec_results,
        loss_missing=loss_missing,
        loss_extra=loss_extra,
        research_results=research_results,
        output_csv=args.output,
    )


if __name__ == "__main__":
    main()
