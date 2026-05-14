"""Check the company's FY configuration in Tally"""
import requests
import re

TALLY_URL = "http://127.0.0.1:9000"
COMPANY = "RUDRAM INC 2024-26"

# Query company info to see FY start/end dates
xml_info = f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>CompanyInfo</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="CompanyInfo" ISMODIFY="No">
        <TYPE>Company</TYPE>
        <NATIVEMETHOD>*</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>'''

try:
    r = requests.post(TALLY_URL, data=xml_info.encode('utf-8'), headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=30)
    body = r.text
    
    # Extract key fields
    fields_to_check = [
        "STARTINGFROM", "BOOKSFROM", "BOOKSCLOSEDAT", "ENDOFLIST",
        "BASICDATEFROM", "BASICDATETO", "STARTDATE", "ENDDATE",
        "BASICCOMPANYNAME", "COMPANYNUMBER", 
        "CURRPERIODSTART", "CURRPERIODFROM", "CURRPERIODTO",
        "BFDATE", "STARTINGFROM", "STARTDATE",
    ]
    
    print(f"Company: {COMPANY}")
    print(f"XML size: {len(body)} bytes")
    print()
    
    # Search for any date-related tags
    date_tags = re.findall(r'<([A-Z]+(?:DATE|FROM|TO|START|END|PERIOD|BOOKS|BEGIN)[A-Z]*)[^>]*>([^<]*)</\1>', body)
    print("Date-related tags found:")
    for tag, val in date_tags:
        print(f"  <{tag}> = {val}")
    
    # Also check STARTING* tags
    starting_tags = re.findall(r'<(STARTING[A-Z]*)[^>]*>([^<]*)</\1>', body)
    for tag, val in starting_tags:
        print(f"  <{tag}> = {val}")
    
    # Check BASICCOMPANYFORMALNAME etc
    basic_tags = re.findall(r'<(BASIC[A-Z]*)[^>]*>([^<]+)</\1>', body)
    print("\nBASIC* tags:")
    for tag, val in basic_tags[:20]:
        print(f"  <{tag}> = {val}")
    
    # Look for BOOKS and FY related info
    print("\nSearching for FY/books/period markers...")
    for keyword in ["BOOK", "PERIOD", "FISCAL", "YEAR", "STARTING", "FROM", "BEGIN"]:
        matches = re.findall(rf'<([A-Z]*{keyword}[A-Z]*)[^>]*>([^<]+)</\1>', body, re.IGNORECASE)
        for tag, val in matches:
            if len(val.strip()) > 0 and len(val.strip()) < 100:
                print(f"  <{tag}> = {val}")

except Exception as e:
    print(f"Error: {e}")

# === Also try setting SVFROMDATE explicitly to FY2024-25 period ===
print("\n" + "=" * 80)
print("TEST: Query with explicit FY period variables")
print("=" * 80)
xml_period = f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>AllVouchers</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE TYPE="Date">20240401</SVFROMDATE>
      <SVTODATE TYPE="Date">20250331</SVTODATE>
      <EXPLODEFLAG>Yes</EXPLODEFLAG>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="AllVouchers" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <CHILDOF>$$VchTypeSales</CHILDOF>
        <NATIVEMETHOD>*</NATIVEMETHOD>
      </COLLECTION>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>'''
try:
    r = requests.post(TALLY_URL, data=xml_period.encode('utf-8'), headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=60)
    body = r.text
    voucher_count = body.count("<VOUCHER ")
    print(f"  XML size: {len(body)} bytes, VOUCHER tags: {voucher_count}")
    if voucher_count > 0:
        dates = re.findall(r'<DATE[^>]*>(\d{8})</DATE>', body)
        print(f"  Dates: {sorted(set(dates))[:10]}")
except Exception as e:
    print(f"  Error: {e}")

# === Try with AlterRange (Tally Prime specific) ===
print("\n" + "=" * 80)
print("TEST: Query with ALTERRANGE for period filtering")
print("=" * 80)
xml_alter = f'''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>VchByPeriod</ID></HEADER>
  <BODY><DESC>
    <STATICVARIABLES>
      <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
      <SVFROMDATE>20240401</SVFROMDATE>
      <SVTODATE>20250331</SVTODATE>
    </STATICVARIABLES>
    <TDL><TDLMESSAGE>
      <COLLECTION NAME="VchByPeriod" ISMODIFY="No">
        <TYPE>Voucher</TYPE>
        <FILTER>DateFilter</FILTER>
        <NATIVEMETHOD>Date</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
        <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
        <NATIVEMETHOD>PartyLedgerName</NATIVEMETHOD>
        <NATIVEMETHOD>Amount</NATIVEMETHOD>
        <NATIVEMETHOD>GUID</NATIVEMETHOD>
      </COLLECTION>
      <SYSTEM TYPE="Formulae" NAME="DateFilter">$$InDateRange:$Date:$SVFROMDATE:$SVTODATE</SYSTEM>
    </TDLMESSAGE></TDL>
  </DESC></BODY>
</ENVELOPE>'''
try:
    r = requests.post(TALLY_URL, data=xml_alter.encode('utf-8'), headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=60)
    body = r.text
    voucher_count = body.count("<VOUCHER ")
    print(f"  XML size: {len(body)} bytes, VOUCHER tags: {voucher_count}")
    if voucher_count > 0:
        dates = re.findall(r'<DATE[^>]*>(\d{8})</DATE>', body)
        print(f"  Dates: {sorted(set(dates))[:10]}")
        types = re.findall(r'<VOUCHERTYPENAME>([^<]+)</VOUCHERTYPENAME>', body)
        from collections import Counter
        print(f"  Voucher types: {dict(Counter(types))}")
except Exception as e:
    print(f"  Error: {e}")
