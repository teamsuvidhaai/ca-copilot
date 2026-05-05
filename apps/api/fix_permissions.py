"""Fix table permissions for voucher_inventory_entries in Supabase."""
import psycopg2

# Direct connection (port 5432, not pooler 6543)
CONN = "postgresql://postgres.yjcbbgjrxvwbdrcprbiy:0gbJc1sNcBYjXRB1@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"

GRANTS = [
    "GRANT ALL ON TABLE voucher_inventory_entries TO service_role;",
    "GRANT ALL ON TABLE voucher_inventory_entries TO authenticated;",
    "GRANT ALL ON TABLE voucher_inventory_entries TO anon;",
    "GRANT ALL ON TABLE voucher_inventory_entries TO postgres;",
    # Also fix RLS — enable but allow service_role full access
    "ALTER TABLE voucher_inventory_entries ENABLE ROW LEVEL SECURITY;",
    "DROP POLICY IF EXISTS service_role_all ON voucher_inventory_entries;",
    "CREATE POLICY service_role_all ON voucher_inventory_entries FOR ALL TO service_role USING (true) WITH CHECK (true);",
    # Also ensure voucher_entries has same permissions (prevent future issues)
    "GRANT ALL ON TABLE voucher_entries TO service_role;",
    "GRANT ALL ON TABLE voucher_entries TO authenticated;",
    "GRANT ALL ON TABLE vouchers TO service_role;",
    "GRANT ALL ON TABLE vouchers TO authenticated;",
    "GRANT ALL ON TABLE ledgers TO service_role;",
    "GRANT ALL ON TABLE ledgers TO authenticated;",
]

def main():
    print("Connecting to Supabase (direct)...")
    conn = psycopg2.connect(CONN)
    conn.autocommit = True
    cur = conn.cursor()

    for sql in GRANTS:
        try:
            cur.execute(sql)
            print(f"  OK: {sql[:60]}...")
        except Exception as e:
            print(f"  FAIL: {sql[:60]}... -> {e}")

    cur.close()
    conn.close()
    print("\nDone! Permissions fixed.")

if __name__ == "__main__":
    main()
