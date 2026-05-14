"""Debug: Check what vouchers exist for RUDRAM in Supabase"""
import httpx, os
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

COMPANY = "RUDRAM INC 2024-26"

# 1. All vouchers
print("=" * 80)
print(f"ALL VOUCHERS for {COMPANY}")
print("=" * 80)
url = f"{SUPABASE_URL}/rest/v1/vouchers?select=id,date,voucher_type,voucher_number,party_name,amount,narration,guid&company_name=eq.{COMPANY.replace(' ', '%20')}&limit=50&order=date.desc"
r = httpx.get(url, headers=headers)
vouchers = r.json()
print(f"Total: {len(vouchers)} vouchers\n")
for v in vouchers:
    date = v.get("date", "?")
    vtype = v.get("voucher_type", "?")
    vnum = v.get("voucher_number", "?")
    party = (v.get("party_name", "") or "")[:40]
    amt = v.get("amount", 0)
    narr = (v.get("narration", "") or "")[:60]
    guid = (v.get("guid", "") or "")[:30]
    print(f"  {date} | {vtype:12s} | #{vnum:8s} | {party:40s} | ₹{amt:>12} | {guid}")
    if narr:
        print(f"    Narration: {narr}")

# 2. All voucher entries
print("\n" + "=" * 80)
print(f"ALL VOUCHER ENTRIES for {COMPANY}")
print("=" * 80)
url2 = f"{SUPABASE_URL}/rest/v1/voucher_entries?select=id,voucher_guid,voucher_date,ledger_name,amount,is_debit&company_name=eq.{COMPANY.replace(' ', '%20')}&limit=50&order=voucher_date.desc"
r2 = httpx.get(url2, headers=headers)
entries = r2.json()
print(f"Total: {len(entries)} entries\n")
for e in entries:
    date = e.get("voucher_date", "?")
    ledger = (e.get("ledger_name", "") or "")[:50]
    amt = e.get("amount", 0)
    debit = "Dr" if e.get("is_debit") else "Cr"
    guid = (e.get("voucher_guid", "") or "")[:30]
    print(f"  {date} | {ledger:50s} | ₹{amt:>12} {debit} | guid: {guid}")

# 3. Check if there are other companies with more vouchers
print("\n" + "=" * 80)
print("VOUCHER COUNTS BY COMPANY")
print("=" * 80)
url3 = f"{SUPABASE_URL}/rest/v1/vouchers?select=company_name&limit=10000"
r3 = httpx.get(url3, headers={**headers, "Range": "0-99999"})
all_vouchers = r3.json()
from collections import Counter
counts = Counter(v.get("company_name", "") for v in all_vouchers)
for company, count in counts.most_common(20):
    print(f"  {company:50s} | {count} vouchers")
