import asyncio
import asyncpg

async def test():
    try:
        conn = await asyncpg.connect(
            'postgresql://postgres.yjcbbgjrxvwbdrcprbiy:0gbJc1sNcBYjXRB1@aws-1-ap-south-1.pooler.supabase.com:6543/postgres',
            ssl='require',
            timeout=15
        )
        result = await conn.fetchval('SELECT count(*) FROM ledgers')
        print(f'Ledger count: {result}')
        await conn.close()
    except Exception as e:
        print(f'Error: {type(e).__name__}: {e}')

asyncio.run(test())
