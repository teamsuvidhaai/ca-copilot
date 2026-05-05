import asyncio, json
from sqlalchemy import text
from app.db.session import AsyncSessionLocal

async def check():
    async with AsyncSessionLocal() as db:
        # Check taxpnl transaction format
        r = await db.execute(text(
            "SELECT structured_data FROM fi_uploads "
            "WHERE instrument_type = 'demat_taxpnl' AND je_status = 'approved' LIMIT 1"
        ))
        row = r.fetchone()
        if not row:
            print("No approved taxpnl")
            return
        sd = dict(row._mapping)['structured_data']
        txns = sd.get('transactions', [])
        print(f"taxpnl transactions: {len(txns)}")
        if txns:
            t = txns[0]
            print(f"First txn keys: {list(t.keys())}")
            print(f"First txn: {json.dumps(t, indent=2, default=str)}")
            print(f"\namount: {t.get('amount')}")
            print(f"quantity: {t.get('quantity')}")
            print(f"price: {t.get('price')}")
            print(f"type: {t.get('type')}")
            print(f"scrip_name: {t.get('scrip_name')}")

asyncio.run(check())
