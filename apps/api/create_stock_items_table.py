"""Create stock_items table in Supabase for Tally master stock item data."""
import psycopg2

CONN = "postgresql://postgres.yjcbbgjrxvwbdrcprbiy:0gbJc1sNcBYjXRB1@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"

SQL = [
    # Create the table
    """
    CREATE TABLE IF NOT EXISTS stock_items (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        company_name TEXT NOT NULL,
        name TEXT NOT NULL,
        parent TEXT,
        category TEXT,
        uom TEXT,
        opening_balance_qty NUMERIC,
        opening_balance_rate NUMERIC,
        opening_balance_value NUMERIC,
        hsn_code TEXT,
        gst_rate NUMERIC,
        description TEXT,
        synced_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(company_name, name)
    );
    """,
    # Indexes
    "CREATE INDEX IF NOT EXISTS idx_stock_items_company ON stock_items(company_name);",
    "CREATE INDEX IF NOT EXISTS idx_stock_items_parent ON stock_items(company_name, parent);",
    "CREATE INDEX IF NOT EXISTS idx_stock_items_hsn ON stock_items(company_name, hsn_code);",

    # Permissions
    "GRANT ALL ON TABLE stock_items TO service_role;",
    "GRANT ALL ON TABLE stock_items TO authenticated;",
    "GRANT ALL ON TABLE stock_items TO anon;",
    "GRANT ALL ON TABLE stock_items TO postgres;",

    # RLS
    "ALTER TABLE stock_items ENABLE ROW LEVEL SECURITY;",
    "DROP POLICY IF EXISTS service_role_all ON stock_items;",
    "CREATE POLICY service_role_all ON stock_items FOR ALL TO service_role USING (true) WITH CHECK (true);",
]

def main():
    print("Connecting to Supabase...")
    conn = psycopg2.connect(CONN)
    conn.autocommit = True
    cur = conn.cursor()

    for sql in SQL:
        try:
            cur.execute(sql)
            label = sql.strip().split('\n')[0][:60]
            print(f"  OK: {label}...")
        except Exception as e:
            label = sql.strip().split('\n')[0][:60]
            print(f"  FAIL: {label}... -> {e}")

    cur.close()
    conn.close()
    print("\nDone! stock_items table created with permissions.")

if __name__ == "__main__":
    main()
