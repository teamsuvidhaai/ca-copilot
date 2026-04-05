"""
Populate SecurityMaster with real sector and market cap data.
Uses NSE/BSE classification for Indian listed stocks.
Run: python populate_security_master.py
"""
import asyncio
from sqlalchemy import select, text
from app.db.session import AsyncSessionLocal
from app.models.models import SecurityMaster

# ─── Real sector & market cap data for Indian stocks ───
# Source: NSE sectoral classification + SEBI market cap categories (Jun 2025)
# Large cap = Top 100 by market cap
# Mid cap = 101-250
# Small cap = 251-500
# Micro cap = 500+

STOCK_DATA = {
    # ── Banking ──
    "HDFC BANK LTD":                    ("Banking", "large_cap"),
    "ICICI BANK LTD":                   ("Banking", "large_cap"),
    "STATE BANK OF INDIA":              ("Banking", "large_cap"),
    "FEDERAL BANK LTD":                 ("Banking", "mid_cap"),
    "IDFC FIRST BANK LTD":              ("Banking", "mid_cap"),
    "UJJIVAN SMALL FINANCE BANK LTD":   ("Banking", "small_cap"),

    # ── Financial Services ──
    "IIFL FINANCE LTD":                 ("Financial Services", "mid_cap"),
    "MAX FINANCIAL SERVICES LTD":       ("Financial Services", "mid_cap"),
    "MOTILAL OSWAL FINANCIAL SERVICES LTD": ("Financial Services", "mid_cap"),
    "360 ONE WAM LIMITED":              ("Financial Services", "mid_cap"),
    "ADITYA BIRLA SUN LIFE AMC LTD":    ("Financial Services", "mid_cap"),
    "PNB HOUSING FINANCE LTD":          ("Financial Services", "mid_cap"),

    # ── IT / Technology ──
    "HCL TECHNOLOGIES LTD":             ("IT / Technology", "large_cap"),
    "MASTEK LTD":                       ("IT / Technology", "small_cap"),

    # ── Oil & Gas / Energy ──
    "VEDANTA LTD":                      ("Metals & Mining", "large_cap"),
    "BHARAT PETROLEUM CORPORATION LTD": ("Oil & Gas", "large_cap"),

    # ── Metals & Mining ──
    "JINDAL STEEL and POWER LTD":       ("Metals & Mining", "large_cap"),
    "JINDAL STAINLESS LTD":             ("Metals & Mining", "mid_cap"),
    "SARDA ENERGY and MINERALS LTD":    ("Metals & Mining", "small_cap"),

    # ── Telecom ──
    "BHARTI AIRTEL LTD":                ("Telecom", "large_cap"),
    "BHARTI AIRTEL PP LIMITED":         ("Telecom", "large_cap"),

    # ── Conglomerate / Infrastructure ──
    "ADANI ENTERPRISES LTD":            ("Conglomerate", "large_cap"),
    "ADANI ENTERPRISES LTD RS 0.50 PARTLY PAID": ("Conglomerate", "large_cap"),
    "ADANI ENTERPRISES LTD RS 0.75 PARTLY PAID": ("Conglomerate", "large_cap"),
    "Adani Enterprises Limited-Rights Issue Application": ("Conglomerate", "large_cap"),
    "GRASIM INDUSTRIES LTD":            ("Cement & Building Materials", "large_cap"),
    "TATA COMMUNICATIONS LTD":          ("Telecom", "mid_cap"),

    # ── Pharma & Healthcare ──
    "ALKEM LABORATORIES LTD":           ("Pharma & Healthcare", "mid_cap"),
    "JUBILANT PHARMOVA LTD":            ("Pharma & Healthcare", "small_cap"),
    "SHILPA MEDICARE LTD":              ("Pharma & Healthcare", "small_cap"),
    "STRIDES PHARMA SCIENCE LTD":       ("Pharma & Healthcare", "small_cap"),
    "TTK HEALTHCARE LTD":               ("Pharma & Healthcare", "small_cap"),

    # ── Consumer / FMCG ──
    "HERITAGE FOODS LTD":               ("FMCG / Consumer", "small_cap"),
    "RADICO KHAITAN LTD":               ("FMCG / Consumer", "mid_cap"),
    "LT FOODS LTD":                     ("FMCG / Consumer", "small_cap"),
    "TILAKNAGAR INDUSTRIES LTD":        ("FMCG / Consumer", "small_cap"),

    # ── Cement & Construction ──
    "BIRLA CORPORATION LTD":            ("Cement & Building Materials", "small_cap"),
    "JK LAKSHMI CEMENT LTD":            ("Cement & Building Materials", "small_cap"),
    "DCM SHRIRAM LTD":                  ("Diversified / Chemicals", "mid_cap"),

    # ── Building Materials / Consumer Durables ──
    "KAJARIA CERAMICS LTD":             ("Building Materials", "mid_cap"),
    "STYLAM INDUSTRIES LTD":            ("Building Materials", "small_cap"),

    # ── Capital Goods / Industrials ──
    "EMMVEE PHOTOVOLTAIC POWER LTD":    ("Renewable Energy", "micro_cap"),
    "CRIZAC LTD":                       ("Engineering / Capital Goods", "micro_cap"),

    # ── Travel & Leisure ──
    "THOMAS COOK I LTD":                ("Travel & Leisure", "small_cap"),

    # ── Healthcare / Life Sciences ──
    "Indegene Limited":                 ("IT / Healthcare Tech", "mid_cap"),

    # ── Special ──
    "Tax Deducted at Source":           (None, None),  # Not a security
}


async def populate():
    async with AsyncSessionLocal() as db:
        # First, add the columns if they don't exist
        try:
            await db.execute(text(
                "ALTER TABLE security_master ADD COLUMN IF NOT EXISTS sector VARCHAR(100)"
            ))
            await db.execute(text(
                "ALTER TABLE security_master ADD COLUMN IF NOT EXISTS market_cap_category VARCHAR(20)"
            ))
            await db.commit()
            print("✅ Columns added/verified")
        except Exception as e:
            await db.rollback()
            print(f"Column add skipped: {e}")

        # Fetch all securities
        securities = (await db.execute(select(SecurityMaster))).scalars().all()
        print(f"\n📊 Found {len(securities)} securities in master table\n")

        updated = 0
        missing = []

        for sec in securities:
            name = sec.name.strip()

            # Direct match
            if name in STOCK_DATA:
                sector, cap = STOCK_DATA[name]
                sec.sector = sector
                sec.market_cap_category = cap
                updated += 1
                print(f"  ✅ {name:<50} → {sector or 'N/A':<30} | {cap or 'N/A'}")
            else:
                # Try case-insensitive match
                matched = False
                for key, (sector, cap) in STOCK_DATA.items():
                    if key.lower() == name.lower():
                        sec.sector = sector
                        sec.market_cap_category = cap
                        updated += 1
                        matched = True
                        print(f"  ✅ {name:<50} → {sector or 'N/A':<30} | {cap or 'N/A'}")
                        break
                if not matched:
                    missing.append(name)
                    print(f"  ❌ {name:<50} → NOT FOUND in reference data")

        await db.commit()

        print(f"\n{'='*70}")
        print(f"Updated: {updated}/{len(securities)}")
        if missing:
            print(f"\nMissing ({len(missing)}):")
            for m in missing:
                print(f"  - {m}")
        print(f"{'='*70}")


if __name__ == "__main__":
    asyncio.run(populate())
