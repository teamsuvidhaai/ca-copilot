"""Run the fy_period migration against Supabase"""
import asyncio, os
from dotenv import load_dotenv
load_dotenv()

async def run_migration():
    import asyncpg
    db_url = os.getenv("DATABASE_URL").replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(db_url)
    
    print("Step 1: Adding fy_period column...")
    await conn.execute("ALTER TABLE ledgers ADD COLUMN IF NOT EXISTS fy_period TEXT;")
    print("  Done")
    
    print("Step 2: Dropping old constraint...")
    await conn.execute("ALTER TABLE ledgers DROP CONSTRAINT IF EXISTS uq_ledgers_company_name;")
    print("  Done")
    
    print("Step 3: Adding new constraint (company_name, name, fy_period)...")
    try:
        await conn.execute("""
            ALTER TABLE ledgers ADD CONSTRAINT uq_ledgers_company_name_fy 
            UNIQUE (company_name, name, fy_period);
        """)
        print("  Done")
    except Exception as e:
        if "already exists" in str(e):
            print("  Already exists - OK")
        else:
            print(f"  Error: {e}")
    
    print("Step 4: Adding FY index...")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_ledgers_fy ON ledgers (company_name, fy_period);")
    print("  Done")
    
    # Verify
    row = await conn.fetchrow("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'ledgers' AND column_name = 'fy_period'
    """)
    if row:
        print(f"\nVerified: column '{row['column_name']}' type={row['data_type']}")
    
    constraints = await conn.fetch("""
        SELECT constraint_name FROM information_schema.table_constraints 
        WHERE table_name = 'ledgers' AND constraint_type = 'UNIQUE'
    """)
    print("Unique constraints:")
    for c in constraints:
        print(f"  - {c['constraint_name']}")
    
    await conn.close()
    print("\nMigration complete!")

asyncio.run(run_migration())
