"""Debug script: Check FI ledger balances and voucher entries for RUDRAM INC 2024-26
to understand why the FI dashboard shows same values across fiscal years."""
import httpx, os, json
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

COMPANY = "RUDRAM INC 2024-26"

# 1. Check FI ledgers under 'Investments' parent/primary_group
print("=" * 80)
print(f"FI LEDGER BALANCES for {COMPANY}")
print("=" * 80)
url = f"{SUPABASE_URL}/rest/v1/ledgers?select=name,parent,primary_group,opening_balance,closing_balance&company_name=eq.{COMPANY.replace(' ', '%20')}&or=(primary_group.eq.Investments,parent.eq.Investments)&limit=50"
r = httpx.get(url, headers=headers)
ledgers = r.json()
print(f"\nFound {len(ledgers)} investment ledgers:")
for l in ledgers:
    name = (l.get("name") or "?")[:40]
    ob = l.get("opening_balance") or 0
    cb = l.get("closing_balance") or 0
    parent = l.get("parent") or ""
    pg = l.get("primary_group") or ""
    print(f"  {name:40s} | OB: {ob:>15} | CB: {cb:>15} | parent: {parent} | pg: {pg}")

# 2. Check voucher_entries for these investment ledgers
print("\n" + "=" * 80)
print("VOUCHER ENTRIES touching investment ledgers")
print("=" * 80)
inv_names = [l["name"] for l in ledgers if l.get("name")]
for lname in inv_names[:5]:  # check first 5
    url2 = f"{SUPABASE_URL}/rest/v1/voucher_entries?select=voucher_guid,voucher_date,amount,is_debit,ledger_name&company_name=eq.{COMPANY.replace(' ', '%20')}&ledger_name=eq.{lname.replace(' ', '%20').replace('&', '%26')}&limit=20&order=voucher_date"
    r2 = httpx.get(url2, headers=headers)
    entries = r2.json()
    if entries:
        print(f"\n  Ledger: {lname} ({len(entries)} entries)")
        for e in entries:
            vdate = e.get("voucher_date") or "NO DATE"
            amt = e.get("amount") or 0
            is_dr = e.get("is_debit")
            print(f"    {vdate} | amount: {amt:>15} | is_debit: {is_dr}")

# 3. Count voucher entries by date range for FY 2024-25 vs 2025-26
print("\n" + "=" * 80)
print("VOUCHER ENTRY COUNT BY FY")
print("=" * 80)

# FY 2024-25: 20240401 to 20250331
url_fy1 = f"{SUPABASE_URL}/rest/v1/voucher_entries?select=count&company_name=eq.{COMPANY.replace(' ', '%20')}&voucher_date=gte.20240401&voucher_date=lte.20250331"
r_fy1 = httpx.get(url_fy1, headers={**headers, "Prefer": "count=exact"})
print(f"FY 2024-25 entries: {r_fy1.headers.get('content-range', 'N/A')}")

# FY 2025-26: 20250401 to 20260331
url_fy2 = f"{SUPABASE_URL}/rest/v1/voucher_entries?select=count&company_name=eq.{COMPANY.replace(' ', '%20')}&voucher_date=gte.20250401&voucher_date=lte.20260331"
r_fy2 = httpx.get(url_fy2, headers={**headers, "Prefer": "count=exact"})
print(f"FY 2025-26 entries: {r_fy2.headers.get('content-range', 'N/A')}")

# 4. Check sum of voucher entries for investment ledgers per FY
print("\n" + "=" * 80)
print("INVESTMENT LEDGER MOVEMENTS BY FY (from voucher_entries)")
print("=" * 80)
for lname in inv_names[:5]:
    encoded = lname.replace(' ', '%20').replace('&', '%26')
    # FY 2024-25
    url_s1 = f"{SUPABASE_URL}/rest/v1/voucher_entries?select=amount&company_name=eq.{COMPANY.replace(' ', '%20')}&ledger_name=eq.{encoded}&voucher_date=gte.20240401&voucher_date=lte.20250331"
    r_s1 = httpx.get(url_s1, headers=headers)
    sum1 = sum(float(e.get("amount", 0) or 0) for e in r_s1.json())
    
    # FY 2025-26
    url_s2 = f"{SUPABASE_URL}/rest/v1/voucher_entries?select=amount&company_name=eq.{COMPANY.replace(' ', '%20')}&ledger_name=eq.{encoded}&voucher_date=gte.20250401&voucher_date=lte.20260331"
    r_s2 = httpx.get(url_s2, headers=headers)
    sum2 = sum(float(e.get("amount", 0) or 0) for e in r_s2.json())
    
    # Before FY 2024-25
    url_s0 = f"{SUPABASE_URL}/rest/v1/voucher_entries?select=amount&company_name=eq.{COMPANY.replace(' ', '%20')}&ledger_name=eq.{encoded}&voucher_date=lt.20240401"
    r_s0 = httpx.get(url_s0, headers=headers)
    sum0 = sum(float(e.get("amount", 0) or 0) for e in r_s0.json())

    print(f"\n  {lname}:")
    print(f"    Before FY 24-25 movement: {sum0:>15.2f}")
    print(f"    FY 2024-25 movement:      {sum1:>15.2f}")
    print(f"    FY 2025-26 movement:      {sum2:>15.2f}")

# 5. Check vouchers (not entries) date range
print("\n" + "=" * 80)
print("VOUCHER DATE RANGE")
print("=" * 80)
url_vmin = f"{SUPABASE_URL}/rest/v1/vouchers?select=date&company_name=eq.{COMPANY.replace(' ', '%20')}&order=date.asc&limit=1"
url_vmax = f"{SUPABASE_URL}/rest/v1/vouchers?select=date&company_name=eq.{COMPANY.replace(' ', '%20')}&order=date.desc&limit=1"
r_min = httpx.get(url_vmin, headers=headers)
r_max = httpx.get(url_vmax, headers=headers)
min_data = r_min.json()
max_data = r_max.json()
print(f"  Earliest voucher date: {min_data[0].get('date') if min_data else 'NONE'}")
print(f"  Latest voucher date:   {max_data[0].get('date') if max_data else 'NONE'}")
