import requests, time
print("Waiting 5s then checking Tally port 9000...")
time.sleep(5)
try:
    r = requests.post('http://127.0.0.1:9000', 
        data=b'<?xml version="1.0"?><ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>C</ID></HEADER><BODY><DESC><STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES><TDL><TDLMESSAGE><COLLECTION NAME="C" ISMODIFY="No"><TYPE>Company</TYPE><NATIVEMETHOD>Name</NATIVEMETHOD></COLLECTION></TDLMESSAGE></TDL></DESC></BODY></ENVELOPE>',
        headers={'Content-Type':'text/xml'}, timeout=10)
    print(f"Tally OK! {len(r.text)} bytes")
except Exception as e:
    print(f"Tally not ready: {e}")
