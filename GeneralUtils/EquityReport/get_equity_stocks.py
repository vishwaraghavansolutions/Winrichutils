import frappe, csv, sys

frappe.init(site="winsight.com", sites_path="/home/ubuntu/winrich/frappe-bench/sites")
frappe.connect()

rows = frappe.db.sql("""
    SELECT DISTINCT cws.stockid, s.stock_name, s.industry, s.sector, s.market_cap_category
    FROM "tabCustomer_Winwize_Stocks" cws
    LEFT JOIN "tabStocks" s ON s.name = cws.stockid
    WHERE cws.holdingqty > 0
      AND LOWER(COALESCE(s.sector, '')) NOT IN ('etf','gold','silver','reit','invit')
    ORDER BY s.sector, cws.stockid
""", as_dict=True)

w = csv.writer(sys.stdout)
w.writerow(["nse_code", "stock_name", "industry", "sector", "market_cap"])
for r in rows:
    w.writerow([r["stockid"], r.get("stock_name",""), r.get("industry",""), r.get("sector",""), r.get("market_cap_category","")])

sys.stderr.write(f"Total equity stocks: {len(rows)}\n")
frappe.destroy()
