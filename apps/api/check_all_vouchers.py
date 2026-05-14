"""Check ALL vouchers in the DB — any company"""
import requests

SUPABASE_URL = "https://yjcbbgjrxvwbdrcprbiy.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlqY2JiZ2pyeHZ3YmRyY3ByYml5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NzA4NTUyNiwiZXhwIjoyMDgyNjYxNTI2fQ.0fEUSYFaiPMBYt-SZKgbVapGtfk-8I5JjQ6fheeCdxA"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

# Get distinct company names with voucher counts
r = requests.get(
    f"{SUPABASE_URL}/rest/v1/vouchers",
    params={
        "select": "company_name,date",
        "order": "company_name,date",
        "limit": "5000",
    },
    headers=headers,
)
data = r.json()
print(f"Total vouchers across ALL companies: {len(data)}")

# Group by company
company_counts = {}
for v in data:
    cn = v.get("company_name", "")
    company_counts.setdefault(cn, {"total": 0, "dates": set()})
    company_counts[cn]["total"] += 1
    company_counts[cn]["dates"].add(v.get("date", ""))

for cn, info in sorted(company_counts.items()):
    dates = sorted(info["dates"])
    print(f"\n  Company: '{cn}' — {info['total']} vouchers")
    print(f"    Date range: {dates[0] if dates else 'N/A'} to {dates[-1] if dates else 'N/A'}")
    # Show FY breakdown
    fy_counts = {}
    for d in dates:
        if len(d) == 8:
            year = int(d[:4])
            month = int(d[4:6])
            fy = f"{year}-{str(year+1)[-2:]}" if month >= 4 else f"{year-1}-{str(year)[-2:]}"
        else:
            fy = "unknown"
        fy_counts[fy] = fy_counts.get(fy, 0) + 1
    for fy in sorted(fy_counts.keys()):
        print(f"    FY {fy}: {fy_counts[fy]} unique dates")
