"""Diagnose the gap between sync (1429) and DB query (89)"""
import os, json
from dotenv import load_dotenv
load_dotenv()

import requests

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# 1. Check ALL distinct company_name values in vouchers table
print("=== All distinct company_names in vouchers ===")
r = requests.get(f"{SUPABASE_URL}/rest/v1/vouchers?select=company_name&limit=10000",
                 headers={**headers, "Range": "0-99999"})
rows = r.json()
from collections import Counter
names = Counter(v.get("company_name", "") for v in rows)
for name, count in names.most_common():
    print(f"  '{name}' -> {count} vouchers")

# 2. Check with exact company name from sync
COMPANY = "RUDRAM INC 2024-26"
print(f"\n=== Exact match: company_name = '{COMPANY}' ===")
r2 = requests.get(f"{SUPABASE_URL}/rest/v1/vouchers?company_name=eq.{COMPANY.replace(' ', '%20')}&select=count",
                  headers={**headers, "Prefer": "count=exact"})
print(f"  Content-Range: {r2.headers.get('content-range', 'N/A')}")
print(f"  Body: {r2.text[:200]}")

# 3. Check with ILIKE (case insensitive / partial)
print(f"\n=== ILIKE match: company_name ILIKE '%RUDRAM%' ===")
r3 = requests.get(f"{SUPABASE_URL}/rest/v1/vouchers?company_name=ilike.*RUDRAM*&select=count",
                  headers={**headers, "Prefer": "count=exact"})
print(f"  Content-Range: {r3.headers.get('content-range', 'N/A')}")

# 4. Show date range and breakdown for exact company
print(f"\n=== Vouchers for '{COMPANY}' by type ===")
r4 = requests.get(f"{SUPABASE_URL}/rest/v1/vouchers?company_name=eq.{COMPANY.replace(' ', '%20')}&select=voucher_type,date&order=date.asc&limit=5000",
                  headers={**headers, "Range": "0-99999"})
vrows = r4.json()
print(f"  Total returned: {len(vrows)}")

if vrows:
    type_counts = Counter(v['voucher_type'] for v in vrows)
    dates = sorted(set(v['date'] for v in vrows))
    print(f"  Date range: {dates[0] if dates else '?'} to {dates[-1] if dates else '?'}")
    print(f"  Unique dates: {len(dates)}")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c}")

# 5. Check if the cleanup deleted them
print(f"\n=== Check if 'synced_at' suggests recent activity ===")
r5 = requests.get(f"{SUPABASE_URL}/rest/v1/vouchers?company_name=eq.{COMPANY.replace(' ', '%20')}&select=synced_at&order=synced_at.desc&limit=3",
                  headers=headers)
for v in r5.json():
    print(f"  Last synced: {v.get('synced_at')}")

# 6. Check the total voucher count across ALL companies  
print(f"\n=== Total vouchers across all companies ===")
r6 = requests.get(f"{SUPABASE_URL}/rest/v1/vouchers?select=count",
                  headers={**headers, "Prefer": "count=exact"})
print(f"  Content-Range: {r6.headers.get('content-range', 'N/A')}")
