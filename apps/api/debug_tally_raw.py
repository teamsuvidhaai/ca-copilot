"""Debug: hit Tally directly to see raw voucher XML for FY 2024-25"""
import requests

BASE = "http://127.0.0.1:17890"

# Check debug-vouchers endpoint to see raw XML
try:
    r = requests.get(f"{BASE}/tally/debug-vouchers", timeout=30)
    data = r.json()
    print(f"Debug vouchers response: ok={data.get('ok')}, company={data.get('company')}")
    if data.get("xml_preview"):
        print(f"\nXML Preview:\n{data['xml_preview']}")
except Exception as e:
    print(f"Error: {e}")

# Also try a direct Tally XML request to count vouchers in FY 2024-25
TALLY_URL = "http://127.0.0.1:9000"
xml_24_25 = '''<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>VoucherCount</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
        <SVCURRENTCOMPANY>RUDRAM INC 2024-26</SVCURRENTCOMPANY>
        <SVFROMDATE>20240401</SVFROMDATE>
        <SVTODATE>20250331</SVTODATE>
      </STATICVARIABLES>
      <TDL>
        <TDLMESSAGE>
          <COLLECTION NAME="VoucherCount" ISMODIFY="No">
            <TYPE>Voucher</TYPE>
            <NATIVEMETHOD>Date</NATIVEMETHOD>
            <NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
            <NATIVEMETHOD>VoucherNumber</NATIVEMETHOD>
            <NATIVEMETHOD>PartyLedgerName</NATIVEMETHOD>
            <NATIVEMETHOD>Amount</NATIVEMETHOD>
            <NATIVEMETHOD>GUID</NATIVEMETHOD>
          </COLLECTION>
        </TDLMESSAGE>
      </TDL>
    </DESC>
  </BODY>
</ENVELOPE>'''

try:
    r = requests.post(TALLY_URL, data=xml_24_25, headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=30)
    body = r.text
    # Count VOUCHER tags
    voucher_count = body.count("<VOUCHER ")
    print(f"\nDirect Tally query FY 2024-25: Found {voucher_count} <VOUCHER> tags in {len(body)} bytes")
    # Show first 3000 chars
    print(f"\nRaw XML (first 3000 chars):\n{body[:3000]}")
except Exception as e:
    print(f"Direct Tally error: {e}")

# Also try FY 2025-26
xml_25_26 = xml_24_25.replace("20240401", "20250401").replace("20250331", "20260331")
try:
    r = requests.post(TALLY_URL, data=xml_25_26, headers={"Content-Type": "text/xml;charset=utf-8"}, timeout=30)
    body = r.text
    voucher_count = body.count("<VOUCHER ")
    print(f"\nDirect Tally query FY 2025-26: Found {voucher_count} <VOUCHER> tags in {len(body)} bytes")
except Exception as e:
    print(f"Direct Tally error: {e}")
