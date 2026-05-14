"""Test: Use SVPERIOD and other Tally Prime period mechanisms to access historical FY data"""
import requests
import re
from collections import Counter

TALLY_URL = "http://127.0.0.1:9000"
COMPANY = "RUDRAM INC 2024-26"

def test_xml(label, xml, timeout=90):
    print(f"\n{'='*80}")
    print(f"TEST: {label}")
    print(f"{'='*80}")
    try:
        r = requests.post(TALLY_URL, data=xml.encode('utf-8'), 
                          headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=timeout)
        body = r.text
        voucher_count = body.count("<VOUCHER ")
        print(f"  XML size: {len(body)} bytes, VOUCHER tags: {voucher_count}")
        if voucher_count > 0:
            dates = re.findall(r'<DATE[^>]*>(\d{8})</DATE>', body)
            unique_dates = sorted(set(dates))
            print(f"  Unique dates: {len(unique_dates)} — {unique_dates[:15]}{'...' if len(unique_dates) > 15 else ''}")
            types = re.findall(r'<VOUCHERTYPENAME>([^<]+)</VOUCHERTYPENAME>', body)
            print(f"  Voucher types: {dict(Counter(types))}")
        if voucher_count == 0 and len(body) < 2000:
            print(f"  Response preview: {body[:1000]}")
        return voucher_count
    except Exception as e:
        print(f"  Error: {e}")
        return -1

# Test A: Use SVCURRENTCOMPANY + explicit SVFROMDATE in YYYYMMDD + AllLedgerEntries removed
test_xml("Minimal query — just Date,VoucherTypeName — FY 2024-25", f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>VchTest</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE>20240401</SVFROMDATE>
      <SVTODATE>20250331</SVTODATE>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="VchTest" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <NATIVEMETHOD>Date</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
        <NATIVEMETHOD>Amount</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>''')

# Test B: Use EXPLODEFLAG
test_xml("With EXPLODEFLAG=Yes — FY 2024-25", f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>VchTest</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE>20240401</SVFROMDATE>
      <SVTODATE>20250331</SVTODATE>
      <EXPLODEFLAG>Yes</EXPLODEFLAG>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="VchTest" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <NATIVEMETHOD>Date</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
        <NATIVEMETHOD>Amount</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>''')

# Test C: Set SVPERIOD explicitly
test_xml("With SVPERIOD=FY 2024-25", f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>VchTest</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVPERIOD>01-Apr-2024 to 31-Mar-2025</SVPERIOD>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="VchTest" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <NATIVEMETHOD>Date</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
        <NATIVEMETHOD>Amount</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>''')

# Test D: Set SVFROMDATE + SVTODATE to full company range (2024-2026)
test_xml("Full company range 2024-2026", f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>VchTest</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE>20240401</SVFROMDATE>
      <SVTODATE>20260331</SVTODATE>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="VchTest" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <NATIVEMETHOD>Date</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
        <NATIVEMETHOD>Amount</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>''')

# Test E: Try without SVCURRENTCOMPANY (use whatever Tally has active)
test_xml("Without SVCURRENTCOMPANY (use active company) — full range", f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>VchTest</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVFROMDATE>20240401</SVFROMDATE>
      <SVTODATE>20260331</SVTODATE>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="VchTest" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <NATIVEMETHOD>Date</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
        <NATIVEMETHOD>Amount</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>''')

# Test F: Try TYPE=Data with Day Book (the actual Tally report) 
test_xml("Day Book report export — FY 2024-25", f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Data</TYPE><ID>Day Book</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE>20240401</SVFROMDATE>
      <SVTODATE>20240430</SVTODATE>
    </STATICVARIABLES>
  </DESC></BODY>
</ENVELOPE>''')

# Test G: SVCURRENTDATE approach
test_xml("With SVCURRENTDATE = 20240401", f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>VchTest</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE>20240401</SVFROMDATE>
      <SVTODATE>20250331</SVTODATE>
      <SVCURRENTDATE>20240401</SVCURRENTDATE>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="VchTest" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <NATIVEMETHOD>Date</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
        <NATIVEMETHOD>Amount</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>''')
