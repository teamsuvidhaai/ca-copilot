"""Check what got synced to DB and diagnose the 119 voucher issue"""
import os, json
from dotenv import load_dotenv
load_dotenv()

import requests

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# 1. Count vouchers by date
print("=== Vouchers in DB by date (top 20) ===")
r = requests.get(f"{SUPABASE_URL}/rest/v1/vouchers?company_name=eq.RUDRAM%20INC%202024-26&select=date,voucher_type&order=date.asc&limit=1000", headers=headers)
rows = r.json()
print(f"Total rows returned: {len(rows)}")

from collections import Counter
date_counts = Counter(v['date'] for v in rows)
type_counts = Counter(v['voucher_type'] for v in rows)

print("\nBy date:")
for d, c in sorted(date_counts.items())[:20]:
    print(f"  {d}: {c} vouchers")

print(f"\nBy type:")
for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
    print(f"  {t}: {c}")

# 2. Check total
r2 = requests.get(f"{SUPABASE_URL}/rest/v1/vouchers?company_name=eq.RUDRAM%20INC%202024-26&select=count", 
                   headers={**headers, "Prefer": "count=exact"})
print(f"\nTotal vouchers in DB: {r2.headers.get('content-range', 'unknown')}")

# 3. Show sample vouchers
print("\n=== Sample vouchers (first 5) ===")
r3 = requests.get(f"{SUPABASE_URL}/rest/v1/vouchers?company_name=eq.RUDRAM%20INC%202024-26&select=date,voucher_type,voucher_number,party_name,amount,guid&order=date.asc&limit=5", headers=headers)
for v in r3.json():
    print(f"  {v['date']} | {v['voucher_type']:15} | #{v.get('voucher_number','?'):6} | {v.get('party_name',''):20} | {v.get('amount',0):>12} | guid={v.get('guid','')[:20]}...")
