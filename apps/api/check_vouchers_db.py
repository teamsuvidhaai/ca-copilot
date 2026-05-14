"""Quick check: what vouchers exist in DB for RUDRAM INC 2024-26"""
import requests

SUPABASE_URL = "https://yjcbbgjrxvwbdrcprbiy.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlqY2JiZ2pyeHZ3YmRyY3ByYml5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NzA4NTUyNiwiZXhwIjoyMDgyNjYxNTI2fQ.0fEUSYFaiPMBYt-SZKgbVapGtfk-8I5JjQ6fheeCdxA"
COMPANY = "RUDRAM INC 2024-26"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

# 1. Get all vouchers for this company
r = requests.get(
    f"{SUPABASE_URL}/rest/v1/vouchers",
    params={
        "select": "date,voucher_type,voucher_number,guid,amount,party_name",
        "company_name": f"eq.{COMPANY}",
        "order": "date.asc",
        "limit": "1000",
    },
    headers=headers,
)
data = r.json()
print(f"Total vouchers in DB for '{COMPANY}': {len(data)}")
print()

# Group by FY
fy_buckets = {}
for v in data:
    d = v.get("date", "")
    # Tally dates are YYYYMMDD
    if len(d) == 8:
        year = int(d[:4])
        month = int(d[4:6])
        if month >= 4:
            fy = f"{year}-{str(year+1)[-2:]}"
        else:
            fy = f"{year-1}-{str(year)[-2:]}"
    else:
        fy = "unknown"
    fy_buckets.setdefault(fy, []).append(v)

print("Vouchers by FY:")
for fy in sorted(fy_buckets.keys()):
    vouchers = fy_buckets[fy]
    print(f"  FY {fy}: {len(vouchers)} vouchers")
    for v in vouchers[:20]:  # show first 20 per FY
        print(f"    {v['date']} | {v['voucher_type']:15s} | #{v['voucher_number']:8s} | {v.get('party_name','')[:30]} | amt={v.get('amount','')}")
    if len(vouchers) > 20:
        print(f"    ... and {len(vouchers)-20} more")
print()

# 2. Check FY 2024-25 specifically (20240401 to 20250331)
fy24 = [v for v in data if v.get("date","") >= "20240401" and v.get("date","") <= "20250331"]
print(f"FY 2024-25 vouchers (date between 20240401 and 20250331): {len(fy24)}")
for v in fy24:
    print(f"  {v['date']} | {v['voucher_type']:15s} | #{v['voucher_number']:8s} | {v.get('party_name','')[:30]} | amt={v.get('amount','')}")

# 3. Check FY 2025-26
fy25 = [v for v in data if v.get("date","") >= "20250401" and v.get("date","") <= "20260331"]
print(f"\nFY 2025-26 vouchers (date between 20250401 and 20260331): {len(fy25)}")
for v in fy25:
    print(f"  {v['date']} | {v['voucher_type']:15s} | #{v['voucher_number']:8s} | {v.get('party_name','')[:30]} | amt={v.get('amount','')}")
