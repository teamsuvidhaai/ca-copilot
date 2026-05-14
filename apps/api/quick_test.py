"""Quick test: 3 targeted Tally queries with 15s timeout each"""
import requests, re
from collections import Counter

TALLY = "http://127.0.0.1:9000"
CO = "RUDRAM INC 2024-26"
T = 15  # timeout seconds

def q(label, xml):
    print(f"\n--- {label} ---")
    try:
        r = requests.post(TALLY, data=xml.encode('utf-8'),
                          headers={"Content-Type":"text/xml;charset=utf-8"}, timeout=T)
        b = r.text
        n = b.count("<VOUCHER ")
        dates = sorted(set(re.findall(r'<DATE[^>]*>(\d{8})</DATE>', b)))
        types = dict(Counter(re.findall(r'<VOUCHERTYPENAME>([^<]+)</VOUCHERTYPENAME>', b)))
        print(f"  {n} vouchers, {len(b)} bytes, dates={dates[:10]}, types={types}")
    except Exception as e:
        print(f"  ERROR: {e}")

base = lambda sv: f'''<?xml version="1.0"?><ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>V</ID></HEADER><BODY><DESC><STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT><SVCURRENTCOMPANY>{CO}</SVCURRENTCOMPANY>{sv}</STATICVARIABLES><TDL><TDLMESSAGE><COLLECTION NAME="V" ISMODIFY="No"><TYPE>Voucher</TYPE><NATIVEMETHOD>Date</NATIVEMETHOD><NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD><NATIVEMETHOD>VoucherNumber</NATIVEMETHOD><NATIVEMETHOD>Amount</NATIVEMETHOD></COLLECTION></TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>'''

# 1. Current approach (FY 2024-25)
q("Current query FY24-25", base("<SVFROMDATE>20240401</SVFROMDATE><SVTODATE>20250331</SVTODATE>"))

# 2. Full company range
q("Full range 2024-2026", base("<SVFROMDATE>20240401</SVFROMDATE><SVTODATE>20260331</SVTODATE>"))

# 3. Without SVCURRENTCOMPANY — use active company
q("No SVCURRENTCOMPANY", f'''<?xml version="1.0"?><ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>V</ID></HEADER><BODY><DESC><STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT><SVFROMDATE>20240401</SVFROMDATE><SVTODATE>20260331</SVTODATE></STATICVARIABLES><TDL><TDLMESSAGE><COLLECTION NAME="V" ISMODIFY="No"><TYPE>Voucher</TYPE><NATIVEMETHOD>Date</NATIVEMETHOD><NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD><NATIVEMETHOD>VoucherNumber</NATIVEMETHOD><NATIVEMETHOD>Amount</NATIVEMETHOD></COLLECTION></TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>''')

# 4. SVCURRENTDATE trick
q("SVCURRENTDATE=20240501", base("<SVFROMDATE>20240401</SVFROMDATE><SVTODATE>20250331</SVTODATE><SVCURRENTDATE>20240501</SVCURRENTDATE>"))

# 5. EXPLODEFLAG
q("EXPLODEFLAG=Yes", base("<SVFROMDATE>20240401</SVFROMDATE><SVTODATE>20250331</SVTODATE><EXPLODEFLAG>Yes</EXPLODEFLAG>"))
