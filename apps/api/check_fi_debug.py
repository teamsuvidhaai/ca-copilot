"""Debug: Find how investment data is stored for RUDRAM"""
import httpx, os
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

COMPANY = "RUDRAM INC 2024-26"

# 1. Check ALL distinct parents
print("=" * 80)
print(f"ALL PARENT GROUPS for {COMPANY}")
print("=" * 80)
url = f"{SUPABASE_URL}/rest/v1/ledgers?select=parent&company_name=eq.{COMPANY.replace(' ', '%20')}&limit=500"
r = httpx.get(url, headers=headers)
parents = sorted(set(row.get("parent", "") or "" for row in r.json()))
for p in parents:
    print(f"  {p}")

# 2. Search for ledgers with 'invest' in name or parent
print("\n" + "=" * 80)
print("LEDGERS matching 'invest' in name or parent")
print("=" * 80)
url2 = f"{SUPABASE_URL}/rest/v1/ledgers?select=name,parent,primary_group,opening_balance,closing_balance&company_name=eq.{COMPANY.replace(' ', '%20')}&or=(name.ilike.%25invest%25,parent.ilike.%25invest%25)&limit=50"
r2 = httpx.get(url2, headers=headers)
for l in r2.json():
    name = (l.get("name") or "?")[:50]
    ob = l.get("opening_balance") or 0
    cb = l.get("closing_balance") or 0
    parent = l.get("parent") or ""
    pg = l.get("primary_group") or ""
    print(f"  {name:50s} | OB: {ob:>15} | CB: {cb:>15} | parent: {parent} | pg: {pg}")

# 3. Check all ledgers with non-zero closing balance
print("\n" + "=" * 80)
print("TOP 20 LEDGERS by absolute closing_balance")
print("=" * 80)
url3 = f"{SUPABASE_URL}/rest/v1/ledgers?select=name,parent,primary_group,opening_balance,closing_balance&company_name=eq.{COMPANY.replace(' ', '%20')}&closing_balance=not.is.null&limit=30&order=closing_balance.desc"
r3 = httpx.get(url3, headers=headers)
for l in r3.json():
    name = (l.get("name") or "?")[:50]
    ob = l.get("opening_balance") or 0
    cb = l.get("closing_balance") or 0
    parent = l.get("parent") or ""
    print(f"  {name:50s} | OB: {ob:>15} | CB: {cb:>15} | parent: {parent}")

# Also check largest negative
url3b = f"{SUPABASE_URL}/rest/v1/ledgers?select=name,parent,primary_group,opening_balance,closing_balance&company_name=eq.{COMPANY.replace(' ', '%20')}&closing_balance=not.is.null&limit=20&order=closing_balance.asc"
r3b = httpx.get(url3b, headers=headers)
print("\nLargest negative balances:")
for l in r3b.json():
    name = (l.get("name") or "?")[:50]
    ob = l.get("opening_balance") or 0
    cb = l.get("closing_balance") or 0
    parent = l.get("parent") or ""
    print(f"  {name:50s} | OB: {ob:>15} | CB: {cb:>15} | parent: {parent}")

# 4. Total ledger count
url4 = f"{SUPABASE_URL}/rest/v1/ledgers?select=count&company_name=eq.{COMPANY.replace(' ', '%20')}"
r4 = httpx.get(url4, headers={**headers, "Prefer": "count=exact"})
print(f"\nTotal ledgers: {r4.headers.get('content-range', 'N/A')}")

# 5. Voucher entry count and date distribution
url5 = f"{SUPABASE_URL}/rest/v1/voucher_entries?select=count&company_name=eq.{COMPANY.replace(' ', '%20')}"
r5 = httpx.get(url5, headers={**headers, "Prefer": "count=exact"})
print(f"Total voucher entries: {r5.headers.get('content-range', 'N/A')}")

url6 = f"{SUPABASE_URL}/rest/v1/voucher_entries?select=voucher_date&company_name=eq.{COMPANY.replace(' ', '%20')}&limit=10&order=voucher_date.desc"
r6 = httpx.get(url6, headers=headers)
print(f"Recent entry dates: {[e.get('voucher_date') for e in r6.json()]}")

url7 = f"{SUPABASE_URL}/rest/v1/voucher_entries?select=voucher_date&company_name=eq.{COMPANY.replace(' ', '%20')}&limit=10&order=voucher_date.asc"
r7 = httpx.get(url7, headers=headers)
print(f"Earliest entry dates: {[e.get('voucher_date') for e in r7.json()]}")

# 6. Check voucher count
url8 = f"{SUPABASE_URL}/rest/v1/vouchers?select=count&company_name=eq.{COMPANY.replace(' ', '%20')}"
r8 = httpx.get(url8, headers={**headers, "Prefer": "count=exact"})
print(f"Total vouchers: {r8.headers.get('content-range', 'N/A')}")
