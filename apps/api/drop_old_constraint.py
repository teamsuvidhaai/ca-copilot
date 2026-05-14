import asyncio, os
from dotenv import load_dotenv
load_dotenv()

async def f():
    import asyncpg
    c = await asyncpg.connect(os.getenv("DATABASE_URL").replace("postgresql+asyncpg://", "postgresql://"))
    await c.execute("ALTER TABLE ledgers DROP CONSTRAINT IF EXISTS ledgers_company_name_name_key;")
    print("Dropped old constraint ledgers_company_name_name_key")
    rows = await c.fetch("SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = 'ledgers' AND constraint_type = 'UNIQUE'")
    for r in rows:
        print(f"  Remaining: {r['constraint_name']}")
    await c.close()

asyncio.run(f())
