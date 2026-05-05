import asyncio
from app.db.session import AsyncSessionLocal
from sqlalchemy import text

async def main():
    async with AsyncSessionLocal() as db:
        # Find Kansal client
        r = await db.execute(text("SELECT id, name FROM clients WHERE name ILIKE '%kansal%'"))
        clients = r.fetchall()
        print("=== Kansal Clients ===")
        for row in clients:
            print(f"  id={row[0]}, name={row[1]}")
        
        if clients:
            cid = str(clients[0][0])
            # Check FI uploads for this client
            r2 = await db.execute(text(f"SELECT id, instrument_type, filename, status, journal_entry_count FROM fi_uploads WHERE client_id = '{cid}'"))
            uploads = r2.fetchall()
            print(f"\n=== FI Uploads for {cid} ===")
            if not uploads:
                print("  (none)")
            for row in uploads:
                print(f"  id={row[0]}, type={row[1]}, file={row[2]}, status={row[3]}, entries={row[4]}")

asyncio.run(main())
