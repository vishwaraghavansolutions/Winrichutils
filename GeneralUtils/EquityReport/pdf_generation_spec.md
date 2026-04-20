# Equity Report PDF — Generation Specification
## Purpose
This document describes every data source, formula, filter and fallback used to generate the customer equity report PDF. `equity_checker.py` should use this as the ground truth to replay each value from the DB and flag discrepancies.

---

## 1. API Calls (in order)

| Step | API Method | Key Args | Returns |
|------|-----------|----------|---------|
| 1 | `fetch_Customer_Winwize_Stocks` | `customer_id` | Raw holdings rows (all-time) |
| 2 | `fetch_customers_data` | `customer_id`, `info:1` | `customerDoc` — authoritative aggregates |
| 3 | `fetch_customer_stocks` | `customer_id` | `tabStocks` master data + pre-built pie maps + per-stock `total_val`, `avg_stock_price`, `qty_held` |
| 4 | `generate_performance_report` | `customer_id` | `performance_overview`, `customer_details`, `narrative` |

---

## 2. Holdings Row Mapping (`mapWinwizeHoldingsRowToStockRow`)

Each raw Winwize row is mapped to a report row. Fields used:

| Report Field | Source Field(s) | Priority |
|---|---|---|
| `stock_id` | `cws.stockid` | — |
| `stock_name` | `tabStocks.stock_name` | — |
| `qty` | `cws.holdingqty` | — |
| `cur_val` | `fetch_customer_stocks.total_val` → `holdingqty × currentprice` → `cws.currentvalue` | CS first |
| `inv_amt` | `fetch_customer_stocks.avg_stock_price × qty_held` → `cws.buyamount` | CS first |
| `profit_loss` | `cur_val − inv_amt` | computed |
| `profit_loss_perc` | `(profit_loss / inv_amt) × 100` | computed |
| `sector` | `tabStocks.sector` | — |
| `industry` | `tabStocks.industry` | — |
| `market_cap_bucket` | `tabStocks.market_cap_category` | — |

**`cur_val` and `inv_amt` use `fetch_customer_stocks` as the primary source** — this matches exactly what the Customer Details page "Stock Holdings" table shows (`total_val` for current value, `avg_stock_price × qty_held` for invested amount).

**Filters applied before any section:**
1. `rows = all_rows.filter(r => r.qty > 0)` — exclude zero-qty rows
2. If `fetch_customer_stocks` returned data, also exclude any stock not present in that response (`total_val = 0`) — ensures the report matches the Customer Details page exactly

---

## 3. Portfolio Snapshot KPIs (§02)

All KPIs come from `customerDoc` (Step 2), NOT from summing holdings rows.

| KPI Label | Formula | DB Field |
|---|---|---|
| Total Investment | direct | `customerDoc.total_buy_amt` |
| Current Portfolio Value | direct | `customerDoc.cur_val` |
| Realised Proceeds | direct | `customerDoc.total_sell_amt_winwize_orders` (pinned from `tabCustomers`, not overridden by performance API) |
| Total P&L (₹) | `cur_val + total_sell_amt_winwize_orders − total_buy_amt` | computed |
| Total P&L (%) | `(total_pnl / total_buy_amt) × 100` | computed |

**Checker query:**
```sql
SELECT total_buy_amt, cur_val, total_sell_amt_winwize_orders
FROM "tabCustomers"
WHERE name = '<customer_id>'
```

---

## 4. Performance Table (§01)

Source: `generate_performance_report` → `performance_overview` array.

Each row has: `period`, `portfolio_return` (or variant keys), `nifty500_return`, `alpha`.

Key normalization: the PDF tries these field name aliases in order:
`portfolio_return`, `portfolio_return_pct`, `portfolio_return_percent`, `portfolioPerformance`, `portfolio_performance`, `pf_return`, `return_pct`, `customer_return`

Alpha = `portfolio_return − nifty500_return` for each period.

Always renders exactly 3 rows: **Since Inception**, **Quarterly**, **Annual (1Y)**. If the API does not return an annual row (e.g. account < 1 year old), Annual (1Y) shows `—` for all values.

Quarter badge label is read from `performance_overview[1].period` (the second row, which is the most recent quarter). If it matches `/Q[1-4]/i` it is used directly, otherwise computed from `endDate`.

**Quarter alias:** "Q4 FY 25-26" is treated as identical to "Q1 FY 26-27" for commentary lookups.

---

## 5. Holdings Table (§03)

Top 10 rows by `cur_val` descending (from `fetch_Customer_Winwize_Stocks`, cross-referenced with `fetch_customer_stocks` for current value).

Columns rendered: Stock, Qty · Buy Amount, Current Value, Unrl. P&L, Return.

**Best Performer / Under Watch badges (below table):**
- **All stocks positive** (`worst.profit_loss_perc > 0`): one full-width **BEST PERFORMER** box only — no Under Watch shown.
- **Any stock negative or zero**: two half-width boxes — **BEST PERFORMER** (green) and **UNDER WATCH** (red).
- **Single stock in top 10**: one full-width box — green ("BEST PERFORMER") if return > 0, red ("UNDER WATCH") if ≤ 0.

---

## 6. Sector Allocation Cards (§04)

All rows where `qty > 0` (and present in `fetch_customer_stocks`), sorted by `cur_val` descending.

**Per-sector stock weight formula** — stock weights within a card always sum to the sector's published allocation %:
```
stock_weight = (stock.cur_val / sum_of_cur_val_for_all_stocks_in_sector) × sector_pct
```
This ensures internal consistency (e.g. if sector = 3.1%, all its stocks sum to 3.1%).

---

## 7. Loss Positions (§05)

Subset of active holdings where `profit_loss_perc < 0` AND `cur_val > 0`, sorted by return ascending (worst first), capped at **4 entries**.

**Data sources** — same as Customer Details page:
- **Portfolio weight**: `(row.cur_val / portfolio_total_from_cs) × 100` where `portfolio_total_from_cs = sum(fetch_customer_stocks.total_val)`
- **Current value**: `row.cur_val` (from `fetch_customer_stocks.total_val`)
- **Invested amount**: `row.inv_amt` (from `avg_stock_price × qty_held` via `fetch_customer_stocks`)

Stocks absent from `fetch_customer_stocks` do **not** appear in §05, even if present in the Winwize holdings API.

---

## 8. Stock Research Notes (§06)

Source: `/stock_research.csv` (served from `public/`) — fetched at report generation time by `fetchStockResearchBrowser`.

CSV columns: `nse_code, stock_name, research_note`

Lookup key: `nse_code.toUpperCase()` matched against `row.stock_id.toUpperCase()`.

**Fallback:** If NSE code not found in CSV → card shows `"Stock research not found."`

**Stocks shown:** Top **6** by `cur_val` descending (stocks not in `fetch_customer_stocks` are excluded).

**Card subtitle format:**
- **Precious metals** (industry contains `gold`, `silver`, `metal`, `precious`, `commodity`): shows raw `industry` only (e.g. `gold`, `silver`) — no market cap label.
- **All other stocks**: shows `row.sector · market_cap_bucket` (e.g. `Consumer Discretionary · large cap`) — uses the broad sector matching §04, not the specific industry.

**Card layout:** 2-column grid. Card height is dynamic — sized to fit the full research text with no truncation. Pairs of cards share the same row height. Page breaks are added automatically.

**Checker:** verify that the top 6 `stock_id` values (by current value, qty > 0, present in `fetch_customer_stocks`) each have an entry in `stock_research.csv`.

---

## 9. Market Commentary (§07)

Source priority:
1. `getMarketOverview(quarterBadgeLabel)` from `quarterlyCommentary.js` (curated text keyed by quarter)
2. Fallback: template built from `performance_overview[1].nifty500_return`

Current curated quarter key: `"Q1 FY 26-27"` (alias: `"Q4 FY 25-26"`)
Period displayed in prose: converted to human-readable form e.g. `"Jan – Mar 2026"`

---

## 10. Sector Allocation Commentary (§08)

**Allocation data source (priority order):**
1. `fetch_customer_stocks` → `customerPieMaps.industry_wise_allocation` (pre-built by server)
2. Fallback: computed from active holdings rows using `row.industry` field, grouped and percentage-rounded

**Sector pie legend:** All sectors shown (no entry limit). Font reduced to fit. Palette supports up to 12 distinct colours.

**Sector commentary text (per card, priority order):**
1. AI narrative from `generate_performance_report` → `narrative.sector_commentary[].body` (if present for that sector)
2. Curated text from `getSectorCommentary(quarterBadgeLabel, sectorLabel)` in `quarterlyCommentary.js`
3. Fallback template using `quarterDisplayLabel` (human-readable period)

Sector label matching (case-insensitive substring):
- `pharma` / `health` → pharma_healthcare
- `bank` / `financ` / `nbfc` → banking_finance
- `tech` / `software` / `it` / `information` → technology_it
- `energy` / `power` / `oil` / `gas` → energy_power
- `infra` / `construct` / `cement` / `industrial` → infrastructure
- `auto` / `mobil` → auto_mobility
- `consumer` / `fmcg` / `retail` → consumer_fmcg
- `metal` / `gold` / `silver` / `precious` / `commodity` → metals_commodities
- `etf` / `index` → etf_index

---

## 11. Next Quarter Strategy (§09)

Source: `getNextQuarterStrategy(quarterBadgeLabel, risk_profile)` from `quarterlyCommentary.js`

Risk profile → tier mapping:
| Risk Profile | Tier |
|---|---|
| Aggressive, Moderately Aggressive | `aggressive_tier` |
| Conservative, Moderately Conservative | `conservative_tier` |
| Moderate, Balanced, Diversified (default) | `moderate_tier` |

Bullet 1: `"Maintain the <top_sector> allocation at ~<pct>%, consistent with the portfolio's risk mandate."`
Bullet 2: names the worst-performing stocks (bottom 3 by return, qty > 0).
Bullet 3: `market_context` from next_quarter config, or risk-tier fallback.
Bullet 4: `strategy` from next_quarter config, or risk-tier fallback.

---

## 12. Cap Allocation (§08 sub-section)

Buckets: `Large Cap`, `Mid Cap`, `Small Cap`, `Others`

Source priority (same as sector):
1. `customerPieMaps.market_cap_allocation` from `fetch_customer_stocks`
2. Fallback: computed from `row.market_cap_bucket` across active holdings

`Others` = anything not matching Large/Mid/Small Cap.

---

## 13. Portfolio Total for Weight Calculations

`portfolio_total_from_cs = sum(fetch_customer_stocks.total_val)` across all stocks for the customer.

Used as the denominator for all portfolio weight % calculations (§05, §06, §09). This matches the "Percentage of holding" column on the Customer Details page exactly.

Fallback (if `fetch_customer_stocks` returns no data): `customerDoc.cur_val` from `tabCustomers`.

---

## 14. Equity Checker — Recommended Validations

For each customer, `equity_checker.py` should:

1. **KPI check**: Query `tabCustomers` and verify `total_buy_amt`, `cur_val`, `total_sell_amt_winwize_orders` match PDF values (±1 rupee rounding tolerance).

2. **Holdings count check**: Count stocks in `fetch_customer_stocks` where `total_val > 0`. Should match number of rows in §04 (the report now uses `fetch_customer_stocks` as the authoritative holdings list).

3. **P&L check**: Recompute `total_pnl = cur_val + total_sell_amt_winwize_orders - total_buy_amt` and verify it matches the PDF's Total P&L.

4. **Loss positions check**: From `fetch_customer_stocks` rows with `total_val > 0`, compute `(total_val - avg_stock_price * qty_held) / (avg_stock_price * qty_held) * 100` and verify the same set (return < 0) appears in §05.

5. **Research coverage check**: For the top 6 stocks by `total_val`, verify each NSE code exists in `stock_research.csv`. Log missing codes.

6. **Sector allocation check**: Re-derive sector buckets from `tabStocks.industry` for all active holdings. Compare percentages to PDF §08 (tolerance ±0.5%).

7. **Stock exclusion check**: Verify no stock absent from `fetch_customer_stocks` appears in §05 or §06, even if present in `tabCustomer_Winwize_Stocks` with `holdingqty > 0`.

8. **Under Watch check**: If all top-10 stocks have positive returns, verify §03 shows only a single "BEST PERFORMER" box (no "UNDER WATCH").

---

## 15. DB Tables Referenced

| Table | Purpose |
|---|---|
| `tabCustomers` | Authoritative aggregates (`total_buy_amt`, `cur_val`, `total_sell_amt_winwize_orders`) |
| `tabCustomer_Winwize_Stocks` | Per-stock holdings (`holdingqty`, `avgbuyrate`, `currentprice`) — used for qty filter and top-10 table |
| `tabStocks` | Master data (`stock_name`, `sector`, `industry`, `market_cap_category`) |
| `fetch_customer_stocks` API | Authoritative per-stock values (`total_val`, `avg_stock_price`, `qty_held`) — same source as Customer Details page |

Join: `tabCustomer_Winwize_Stocks.stockid = tabStocks.name`
