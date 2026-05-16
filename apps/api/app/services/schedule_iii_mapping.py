"""
Schedule III Mapping — Single Source of Truth
─────────────────────────────────────────────
Maps Tally parent groups to Schedule III line items per
the Companies Act, 2013. Used by both the Tally direct
generation and the file-upload generation paths.
"""


# ═══════════════════════════════════════════════════════
#  TALLY GROUP → SCHEDULE III MAPPING
# ═══════════════════════════════════════════════════════

# Maps Tally parent groups (lowercase) to (category, schedule_group, note_no)
TALLY_TO_SCHEDULE_III = {
    # ── Equity ──
    'capital account':           ('Equity',    'Share Capital',                 'Note 1'),
    'share capital':             ('Equity',    'Share Capital',                 'Note 1'),
    'reserves & surplus':        ('Equity',    'Reserves & Surplus',            'Note 2'),
    'retained earnings':         ('Equity',    'Reserves & Surplus',            'Note 2'),

    # ── Non-Current Liabilities ──
    'secured loans':             ('Liability', 'Long-Term Borrowings',          'Note 3'),
    'unsecured loans':           ('Liability', 'Long-Term Borrowings',          'Note 3'),
    'loans (liability)':         ('Liability', 'Long-Term Borrowings',          'Note 3'),
    'deferred tax liability':    ('Liability', 'Deferred Tax Liabilities (Net)','Note 4'),
    'long term provisions':      ('Liability', 'Long-Term Provisions',          'Note 5'),

    # ── Current Liabilities ──
    'bank od a/c':               ('Liability', 'Short-Term Borrowings',         'Note 6'),
    'sundry creditors':          ('Liability', 'Trade Payables',                'Note 7'),
    'trade payables':            ('Liability', 'Trade Payables',                'Note 7'),
    'duties & taxes':            ('Liability', 'Other Current Liabilities',     'Note 8'),
    'current liabilities':       ('Liability', 'Other Current Liabilities',     'Note 8'),
    'provisions':                ('Liability', 'Short-Term Provisions',         'Note 9'),

    # ── Non-Current Assets ──
    'fixed assets':              ('Asset',     'Tangible Assets',               'Note 10'),
    'intangible assets':         ('Asset',     'Intangible Assets',             'Note 11'),
    'capital work-in-progress':  ('Asset',     'Capital Work-in-Progress',      'Note 12'),
    'investments':               ('Asset',     'Non-Current Investments',       'Note 13'),
    'long term loans & advances':('Asset',     'Long-Term Loans & Advances',    'Note 14'),

    # ── Current Assets ──
    'stock-in-hand':             ('Asset',     'Inventories',                   'Note 15'),
    'closing stock':             ('Asset',     'Inventories',                   'Note 15'),
    'sundry debtors':            ('Asset',     'Trade Receivables',             'Note 16'),
    'trade receivables':         ('Asset',     'Trade Receivables',             'Note 16'),
    'bank accounts':             ('Asset',     'Cash & Cash Equivalents',       'Note 17'),
    'cash-in-hand':              ('Asset',     'Cash & Cash Equivalents',       'Note 17'),
    'deposits (asset)':          ('Asset',     'Short-Term Loans & Advances',   'Note 18'),
    'loans & advances (asset)':  ('Asset',     'Short-Term Loans & Advances',   'Note 18'),
    'other current assets':      ('Asset',     'Other Current Assets',          'Note 19'),

    # ── Revenue ──
    'sales accounts':            ('Income',    'Revenue from Operations',       'Note 20'),
    'direct income':             ('Income',    'Revenue from Operations',       'Note 20'),
    'indirect income':           ('Income',    'Other Income',                  'Note 21'),

    # ── Expenses ──
    'purchase accounts':         ('Expense',   'Cost of Materials Consumed',    'Note 22'),
    'manufacturing expenses':    ('Expense',   'Cost of Materials Consumed',    'Note 22'),
    'direct expenses':           ('Expense',   'Changes in Inventories',        'Note 23'),
    'employee benefit expense':  ('Expense',   'Employee Benefit Expense',      'Note 24'),
    'salary':                    ('Expense',   'Employee Benefit Expense',      'Note 24'),
    'indirect expenses':         ('Expense',   'Other Expenses',               'Note 27'),
    'depreciation':              ('Expense',   'Depreciation & Amortisation',   'Note 25'),
    'bank charges':              ('Expense',   'Finance Costs',                'Note 26'),
    'interest paid':             ('Expense',   'Finance Costs',                'Note 26'),
    'interest expense':          ('Expense',   'Finance Costs',                'Note 26'),
}


def match_tally_group(parent: str) -> tuple:
    """Fuzzy-match a Tally parent group to Schedule III mapping.
    Returns (category, schedule_group, note_ref)."""
    p = parent.lower().strip()
    # Exact match
    if p in TALLY_TO_SCHEDULE_III:
        return TALLY_TO_SCHEDULE_III[p]
    # Partial match
    for key, val in TALLY_TO_SCHEDULE_III.items():
        if key in p or p in key:
            return val
    # Fallback based on common patterns
    if any(kw in p for kw in ['expense', 'charges', 'rent', 'insurance', 'repairs']):
        return ('Expense', 'Other Expenses', 'Note 27')
    if any(kw in p for kw in ['income', 'revenue', 'receipt']):
        return ('Income', 'Other Income', 'Note 21')
    if any(kw in p for kw in ['loan', 'advance', 'deposit']):
        return ('Asset', 'Short-Term Loans & Advances', 'Note 18')
    if any(kw in p for kw in ['creditor', 'payable']):
        return ('Liability', 'Trade Payables', 'Note 7')
    if any(kw in p for kw in ['debtor', 'receivable']):
        return ('Asset', 'Trade Receivables', 'Note 16')
    return ('Asset', 'Other Current Assets', 'Note 19')


# ═══════════════════════════════════════════════════════
#  SCHEDULE III GROUP ORDERING (for BS / P&L rendering)
# ═══════════════════════════════════════════════════════

BS_EQUITY_LIAB_ORDER = [
    ("Shareholders' Funds", ['Share Capital', 'Reserves & Surplus']),
    ("Non-Current Liabilities", ['Long-Term Borrowings', 'Deferred Tax Liabilities (Net)', 'Long-Term Provisions']),
    ("Current Liabilities", ['Short-Term Borrowings', 'Trade Payables', 'Other Current Liabilities', 'Short-Term Provisions']),
]

BS_ASSETS_ORDER = [
    ("Non-Current Assets", ['Tangible Assets', 'Intangible Assets', 'Capital Work-in-Progress', 'Non-Current Investments', 'Long-Term Loans & Advances']),
    ("Current Assets", ['Inventories', 'Trade Receivables', 'Cash & Cash Equivalents', 'Short-Term Loans & Advances', 'Other Current Assets']),
]

PL_INCOME_ORDER = ['Revenue from Operations', 'Other Income']
PL_EXPENSE_ORDER = [
    'Cost of Materials Consumed', 'Changes in Inventories',
    'Employee Benefit Expense', 'Finance Costs',
    'Depreciation & Amortisation', 'Other Expenses',
]
