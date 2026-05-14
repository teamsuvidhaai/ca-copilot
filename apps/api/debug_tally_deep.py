"""Deep debug: Query Tally directly with different XML approaches to find the vouchers"""
import requests

TALLY_URL = "http://127.0.0.1:9000"
COMPANY = "RUDRAM INC 2024-26"

# === Test 1: Our current query for FY 2024-25 — but dump more XML ===
print("=" * 80)
print("TEST 1: Current XML query (SVFROMDATE/SVTODATE for FY 2024-25)")
print("=" * 80)
xml1 = f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>AllVouchers</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE>20240401</SVFROMDATE>
      <SVTODATE>20250331</SVTODATE>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="AllVouchers" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <NATIVEMETHOD>Date</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
        <NATIVEMETHOD>PartyLedgerName</NATIVEMETHOD>
        <NATIVEMETHOD>Amount</NATIVEMETHOD>
        <NATIVEMETHOD>GUID</NATIVEMETHOD>
        <NATIVEMETHOD>AlterID</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>'''
try:
    r = requests.post(TALLY_URL, data=xml1.encode('utf-8'), headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=60)
    body = r.text
    voucher_count = body.count("<VOUCHER ")
    print(f"  XML size: {len(body)} bytes, VOUCHER tags: {voucher_count}")
    
    # Extract dates from the vouchers
    import re
    dates = re.findall(r'<DATE[^>]*>(\d{8})</DATE>', body)
    print(f"  Voucher dates found: {sorted(set(dates))}")
    
    # Extract voucher types
    types = re.findall(r'<VOUCHERTYPENAME>([^<]+)</VOUCHERTYPENAME>', body)
    from collections import Counter
    print(f"  Voucher types: {dict(Counter(types))}")
except Exception as e:
    print(f"  Error: {e}")

# === Test 2: Try WITHOUT date filter ===
print("\n" + "=" * 80)
print("TEST 2: Query WITHOUT date filter (ALL vouchers)")
print("=" * 80)
xml2 = f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>AllVouchers</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="AllVouchers" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <NATIVEMETHOD>Date</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
        <NATIVEMETHOD>PartyLedgerName</NATIVEMETHOD>
        <NATIVEMETHOD>Amount</NATIVEMETHOD>
        <NATIVEMETHOD>GUID</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>'''
try:
    r = requests.post(TALLY_URL, data=xml2.encode('utf-8'), headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=120)
    body = r.text
    voucher_count = body.count("<VOUCHER ")
    print(f"  XML size: {len(body)} bytes, VOUCHER tags: {voucher_count}")
    dates = re.findall(r'<DATE[^>]*>(\d{8})</DATE>', body)
    print(f"  Voucher dates found: {sorted(set(dates))}")
    types = re.findall(r'<VOUCHERTYPENAME>([^<]+)</VOUCHERTYPENAME>', body)
    print(f"  Voucher types: {dict(Counter(types))}")
except Exception as e:
    print(f"  Error: {e}")

# === Test 3: Use Tally date format (DD-MMM-YYYY) in SVFROMDATE ===
print("\n" + "=" * 80)
print("TEST 3: Tally date format (01-Apr-2024 to 31-Mar-2025)")
print("=" * 80)
xml3 = f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>AllVouchers</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE>01-Apr-2024</SVFROMDATE>
      <SVTODATE>31-Mar-2025</SVTODATE>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="AllVouchers" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <NATIVEMETHOD>Date</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
        <NATIVEMETHOD>Amount</NATIVEMETHOD>
        <NATIVEMETHOD>GUID</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>'''
try:
    r = requests.post(TALLY_URL, data=xml3.encode('utf-8'), headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=60)
    body = r.text
    voucher_count = body.count("<VOUCHER ")
    print(f"  XML size: {len(body)} bytes, VOUCHER tags: {voucher_count}")
    dates = re.findall(r'<DATE[^>]*>(\d{8})</DATE>', body)
    print(f"  Voucher dates found: {sorted(set(dates))}")
    types = re.findall(r'<VOUCHERTYPENAME>([^<]+)</VOUCHERTYPENAME>', body)
    print(f"  Voucher types: {dict(Counter(types))}")
except Exception as e:
    print(f"  Error: {e}")

# === Test 4: Use "Day Book" approach like Tally UI ===
print("\n" + "=" * 80)
print("TEST 4: Day Book collection (like Tally UI)")
print("=" * 80)
xml4 = f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Data</TYPE><ID>Day Book</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE>20240401</SVFROMDATE>
      <SVTODATE>20250331</SVTODATE>
    </STATICVARIABLES>
  </DESC></BODY>
</ENVELOPE>'''
try:
    r = requests.post(TALLY_URL, data=xml4.encode('utf-8'), headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=120)
    body = r.text
    voucher_count = body.count("<VOUCHER ")
    print(f"  XML size: {len(body)} bytes, VOUCHER tags: {voucher_count}")
    dates = re.findall(r'<DATE[^>]*>(\d{8})</DATE>', body)
    unique_dates = sorted(set(dates))
    print(f"  Unique dates: {len(unique_dates)} — {unique_dates[:10]}{'...' if len(unique_dates) > 10 else ''}")
    types = re.findall(r'<VOUCHERTYPENAME>([^<]+)</VOUCHERTYPENAME>', body)
    print(f"  Voucher types: {dict(Counter(types))}")
except Exception as e:
    print(f"  Error: {e}")

# === Test 5: Use FETCH for FY period ===
print("\n" + "=" * 80)
print("TEST 5: Collection with FETCH + CHILDOF filter")
print("=" * 80)
xml5 = f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>VchFY2425</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE>20240401</SVFROMDATE>
      <SVTODATE>20250331</SVTODATE>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="VchFY2425" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <FETCH>Date, VoucherTypeName, VoucherNumber, PartyLedgerName, Amount, GUID, AlterID, Narration</FETCH>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>'''
try:
    r = requests.post(TALLY_URL, data=xml5.encode('utf-8'), headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=120)
    body = r.text
    voucher_count = body.count("<VOUCHER ")
    print(f"  XML size: {len(body)} bytes, VOUCHER tags: {voucher_count}")
    dates = re.findall(r'<DATE[^>]*>(\d{8})</DATE>', body)
    unique_dates = sorted(set(dates))
    print(f"  Unique dates: {len(unique_dates)} — {unique_dates[:10]}{'...' if len(unique_dates) > 10 else ''}")
    types = re.findall(r'<VOUCHERTYPENAME>([^<]+)</VOUCHERTYPENAME>', body)
    print(f"  Voucher types: {dict(Counter(types))}")
except Exception as e:
    print(f"  Error: {e}")
