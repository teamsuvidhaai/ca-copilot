"""Test: Voucher type filter (like HisabKitab)"""
import requests, re, time

TALLY = "http://127.0.0.1:9000"
CO = "RUDRAM INC 2024-26"

types = ["Sales", "Purchase", "Receipt", "Payment", "Contra", "Journal", 
         "Credit Note", "Debit Note", "Sales - Automatic", "Purchase - Automatic"]

print("=== Querying by voucher type (full FY range) ===\n")
total = 0
for vt in types:
    xml = f'''<?xml version="1.0"?>
<ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>V</ID></HEADER>
<BODY><DESC><STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
<SVCURRENTCOMPANY>{CO}</SVCURRENTCOMPANY>
<SVFROMDATE>20240401</SVFROMDATE><SVTODATE>20250331</SVTODATE>
</STATICVARIABLES><TDL><TDLMESSAGE>
<COLLECTION NAME="V" ISMODIFY="No"><TYPE>Voucher</TYPE>
<FILTER>VF</FILTER>
<NATIVEMETHOD>Date</NATIVEMETHOD><NATIVEMETHOD>VoucherTypeName</NATIVEMETHOD>
<NATIVEMETHOD>Amount</NATIVEMETHOD><NATIVEMETHOD>GUID</NATIVEMETHOD>
</COLLECTION>
<SYSTEM TYPE="Formulae" NAME="VF">$VoucherTypeName = "{vt}"</SYSTEM>
</TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>'''
    t0 = time.time()
    try:
        r = requests.post(TALLY, data=xml.encode('utf-8'),
                          headers={'Content-Type':'text/xml;charset=utf-8'}, timeout=120)
        n = r.text.count('<VOUCHER ')
        elapsed = time.time() - t0
        print(f"  {vt:25s} → {n:5d} vouchers, {len(r.text)//1024:5d}KB, {elapsed:.1f}s")
        total += n
    except Exception as e:
        print(f"  {vt:25s} → ERROR ({time.time()-t0:.0f}s)")
    time.sleep(0.5)

print(f"\n  TOTAL: {total} vouchers")
