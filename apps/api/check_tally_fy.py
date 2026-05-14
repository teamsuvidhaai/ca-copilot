"""Check what TallyConnector returns for each FY"""
import requests

BASE = "http://127.0.0.1:17890"
COMPANY = "RUDRAM INC 2024-26"

# Try FY 2024-25
for fy_label, from_d, to_d in [
    ("FY 2024-25", "20240401", "20250331"),
    ("FY 2025-26", "20250401", "20260331"),
    ("FY 2026-27", "20260401", "20270331"),
]:
    try:
        r = requests.get(f"{BASE}/tally/sync-vouchers", params={
            "company": COMPANY,
            "from_date": from_d,
            "to_date": to_d,
        }, timeout=60)
        data = r.json()
        count = data.get("synced", 0)
        print(f"{fy_label} ({from_d}-{to_d}): {count} vouchers synced")
        if count > 0 and "data" in data:
            for v in data["data"][:5]:
                print(f"  date={v.get('date','')} type={v.get('voucher_type','')} num={v.get('voucher_number','')} amt={v.get('amount','')}")
    except Exception as e:
        print(f"{fy_label}: ERROR - {e}")
