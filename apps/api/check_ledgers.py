import httpx, os
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# Check ledgers for RUDRAM
url = f"{SUPABASE_URL}/rest/v1/ledgers?select=name,parent,primary_group&company_name=eq.RUDRAM%20INC%202024-26&limit=15"
r = httpx.get(url, headers=headers)
data = r.json()
print(f"=== RUDRAM Ledgers (first 15 of {len(data)}) ===")
for row in data[:15]:
    name = (row.get("name") or "?")[:35]
    parent = row.get("parent") or ""
    pg = row.get("primary_group") or ""
    print(f"  {name:35s} | parent: {parent:25s} | pg: {pg}")

# Check distinct parents
url2 = f"{SUPABASE_URL}/rest/v1/ledgers?select=parent&company_name=eq.RUDRAM%20INC%202024-26&limit=500"
r2 = httpx.get(url2, headers=headers)
parents = set(row.get("parent", "") for row in r2.json())
print(f"\n=== Distinct parent groups ({len(parents)}) ===")
for p in sorted(parents):
    print(f"  {p}")

# Check if Sundry Debtors/Creditors exist
url3 = f"{SUPABASE_URL}/rest/v1/ledgers?select=name,parent,closing_balance&company_name=eq.RUDRAM%20INC%202024-26&parent=eq.Sundry%20Debtors&limit=5"
r3 = httpx.get(url3, headers=headers)
print(f"\n=== Sundry Debtors ({len(r3.json())}) ===")
for row in r3.json()[:5]:
    print(f"  {row.get('name', '?')}: {row.get('closing_balance')}")

# Check stock_items table
url4 = f"{SUPABASE_URL}/rest/v1/stock_items?select=name,company_name&company_name=eq.RUDRAM%20INC%202024-26&limit=5"
r4 = httpx.get(url4, headers=headers)
print(f"\n=== Stock Items ({len(r4.json())}) ===")
for row in r4.json()[:5]:
    print(f"  {row.get('name', '?')}")
