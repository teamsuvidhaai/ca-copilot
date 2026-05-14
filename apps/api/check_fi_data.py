"""Quick check: Do FI ledgers have VoucherEntry data for FY-specific balances?"""
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

DATABASE_URL = "postgresql+asyncpg://postgres.yjcbbgjrxvwbdrcprbiy:0gbJc1sNcBYjXRB1@aws-1-ap-south-1.pooler.supabase.com:6543/postgres"

async def check():
    engine = create_async_engine(DATABASE_URL, echo=False)
    async with engine.connect() as conn:
        # 1) FI-related ledgers (under Investments parent)
        r = await conn.execute(text("""
            SELECT name, parent, primary_group, opening_balance, closing_balance, company_name
            FROM ledgers
            WHERE lower(primary_group) IN ('investments','current investments','non-current investments')
               OR lower(name) LIKE '%%shares of%%'
               OR lower(name) LIKE '%%mutual fund%%'
            ORDER BY abs(closing_balance) DESC NULLS LAST
            LIMIT 15
        """))
        rows = r.fetchall()
        print(f"=== FI Ledgers: {len(rows)} ===")
        for row in rows:
            print(f"  {row[0][:50]:50s} | OB={row[3]:>12} | CB={row[4]:>12} | co={row[5][:30] if row[5] else ''}")

        # 2) Check VoucherEntries for each FI ledger
        print("\n=== VoucherEntry counts for FI ledgers ===")
        for row in rows[:8]:
            name = row[0]
            company = row[5]
            r2 = await conn.execute(text("""
                SELECT count(*), min(voucher_date), max(voucher_date)
                FROM voucher_entries
                WHERE ledger_name = :name AND company_name = :company
            """), {"name": name, "company": company})
            entry = r2.fetchone()
            print(f"  {name[:45]:45s} | entries={entry[0]:>4} | dates={entry[1]} to {entry[2]}")

        # 3) Check total vouchers by date range to understand what FYs have data
        print("\n=== Voucher date ranges by company ===")
        r3 = await conn.execute(text("""
            SELECT company_name, count(*), min(date), max(date)
            FROM vouchers
            GROUP BY company_name
        """))
        for row in r3.fetchall():
            print(f"  {row[0][:40]:40s} | vouchers={row[1]:>6} | {row[2]} to {row[3]}")

    await engine.dispose()

asyncio.run(check())
