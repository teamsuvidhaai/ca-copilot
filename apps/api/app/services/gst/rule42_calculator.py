"""
Rule 42 CGST — ITC Reversal Calculator Service

Implements the ITC apportionment logic for taxpayers making both
taxable and exempt supplies under CGST Rules, 2017.

Calculation Flow (5 stages):
1. Total ITC (T) minus blocked credits (T3) and exclusive attributions (T1, T2) → C1
2. Mandatory reversals: D1 = T2 (exempt exclusive), D2 = T1 (non-business)
3. Common credit pool: C2 = C1 − D1 − D2
4. Proportionate reversal: C3 = C2 × (E + N) / F
5. Net eligible ITC: C4 = C2 − C3

The provisional monthly C3 must be trued up annually using full-year
turnover figures under Rule 42(2), with adjustment flowing through
the April GSTR-3B of the next year.

References:
  - GSTR-3B Table 4(A): Net eligible ITC (C4)
  - GSTR-3B Table 4(B)(1): D1 reversal
  - GSTR-3B Table 4(B)(2): D2 + C3 reversals
"""

from dataclasses import dataclass, asdict
from typing import Optional
import math


@dataclass
class Rule42Input:
    """Input parameters for Rule 42 ITC reversal calculation."""

    # Step 1: Total ITC
    T: float = 0.0        # Total ITC on inputs/input services
    T3: float = 0.0       # Blocked credit under Section 17(5)

    # Step 2: Exclusively attributable ITC
    T1: float = 0.0       # Exclusively for non-business/personal use
    T2: float = 0.0       # Exclusively for exempt supplies

    # Step 4: Turnover figures
    E: float = 0.0        # Exempt turnover (aggregate value)
    N: float = 0.0        # Non-taxable (non-GST) turnover
    F: float = 0.0        # Total turnover (all supplies)

    # Metadata
    period: Optional[str] = None       # e.g. "2025-04"
    tax_head: Optional[str] = "cgst"   # cgst / sgst / igst


@dataclass
class Rule42Result:
    """Output of Rule 42 ITC reversal calculation."""

    # Intermediate values
    C1: float              # T − T1 − T2 − T3
    D1: float              # = T2 (mandatory reversal for exempt exclusive)
    D2: float              # = T1 (mandatory reversal for non-business)
    C2: float              # Common credit pool (C1 − D1 − D2)
    exempt_ratio: float    # (E + N) / F
    C3: float              # Proportionate reversal (C2 × ratio)
    C4: float              # Net eligible common ITC (C2 − C3)

    # Summary
    total_reversal: float  # D1 + D2 + C3
    net_eligible_itc: float  # C4 (flows to GSTR-3B Table 4(A))

    # GSTR-3B filing references
    gstr3b_4b1: float      # D1 → Table 4(B)(1)
    gstr3b_4b2: float      # D2 + C3 → Table 4(B)(2)

    # Input echo
    inputs: dict

    # Warnings
    warnings: list


def calculate_rule42(inp: Rule42Input) -> Rule42Result:
    """
    Perform Rule 42 CGST ITC reversal calculation.

    Args:
        inp: Rule42Input with all turnover and ITC values.

    Returns:
        Rule42Result with computed reversals, eligible ITC, and GSTR-3B refs.

    Raises:
        ValueError: If inputs are logically invalid (e.g. negative values).
    """
    warnings = []

    # ── Input validation ──
    if inp.T < 0:
        raise ValueError("Total ITC (T) cannot be negative")
    if inp.F < 0:
        raise ValueError("Total turnover (F) cannot be negative")
    if any(v < 0 for v in [inp.T1, inp.T2, inp.T3, inp.E, inp.N]):
        raise ValueError("Input values cannot be negative")

    # ── Business logic warnings ──
    exclusive_sum = inp.T1 + inp.T2 + inp.T3
    if exclusive_sum > inp.T:
        warnings.append(
            f"Exclusive deductions (T1+T2+T3 = ₹{exclusive_sum:,.0f}) exceed "
            f"total ITC (T = ₹{inp.T:,.0f}). C1 will be capped at zero."
        )

    if inp.F > 0 and (inp.E + inp.N) > inp.F:
        warnings.append(
            f"Exempt + Non-taxable turnover (₹{inp.E + inp.N:,.0f}) exceeds "
            f"total turnover (₹{inp.F:,.0f}). Ratio will exceed 100%."
        )

    if inp.F == 0 and (inp.E + inp.N) > 0:
        warnings.append(
            "Total turnover (F) is zero — cannot compute ratio. "
            "C3 reversal will be zero."
        )

    # ── Step 1-2: C1 ──
    C1 = max(0.0, inp.T - inp.T1 - inp.T2 - inp.T3)

    # ── Step 3: Mandatory reversals & common credit ──
    D1 = inp.T2   # ITC exclusively for exempt → reversed
    D2 = inp.T1   # ITC exclusively for non-business → reversed
    C2 = max(0.0, C1 - D1 - D2)

    # ── Step 4: Proportionate reversal ──
    F_safe = inp.F if inp.F > 0 else 1.0
    exempt_ratio = (inp.E + inp.N) / F_safe
    C3 = C2 * exempt_ratio

    # ── Final ──
    C4 = C2 - C3
    total_reversal = D1 + D2 + C3

    return Rule42Result(
        C1=round(C1, 2),
        D1=round(D1, 2),
        D2=round(D2, 2),
        C2=round(C2, 2),
        exempt_ratio=round(exempt_ratio, 6),
        C3=round(C3, 2),
        C4=round(C4, 2),
        total_reversal=round(total_reversal, 2),
        net_eligible_itc=round(C4, 2),
        gstr3b_4b1=round(D1, 2),
        gstr3b_4b2=round(D2 + C3, 2),
        inputs=asdict(inp),
        warnings=warnings,
    )


def calculate_rule42_annual_trueup(
    monthly_results: list[Rule42Result],
    annual_E: float,
    annual_N: float,
    annual_F: float,
) -> dict:
    """
    Rule 42(2) annual true-up computation.

    Compares sum of monthly provisional C3 reversals against
    the reversal that would result from using actual annual
    turnover figures, and produces the adjustment amount.

    Args:
        monthly_results: List of 12 monthly Rule42Result objects.
        annual_E: Actual annual exempt turnover.
        annual_N: Actual annual non-taxable turnover.
        annual_F: Actual annual total turnover.

    Returns:
        Dict with annual_C3, sum_monthly_C3, adjustment, and direction.
    """
    sum_monthly_C3 = sum(r.C3 for r in monthly_results)
    sum_monthly_C2 = sum(r.C2 for r in monthly_results)

    F_safe = annual_F if annual_F > 0 else 1.0
    annual_ratio = (annual_E + annual_N) / F_safe
    annual_C3 = sum_monthly_C2 * annual_ratio

    adjustment = annual_C3 - sum_monthly_C3

    return {
        "rule": "Rule 42(2)",
        "annual_exempt_ratio": round(annual_ratio, 6),
        "sum_monthly_C2": round(sum_monthly_C2, 2),
        "annual_C3_should_be": round(annual_C3, 2),
        "sum_monthly_C3_provisional": round(sum_monthly_C3, 2),
        "adjustment_amount": round(adjustment, 2),
        "direction": "additional_reversal" if adjustment > 0 else "credit_reclaim",
        "file_in": "April GSTR-3B of next financial year",
        "note": (
            "Positive adjustment = additional ITC reversal required in April return. "
            "Negative = excess reversal done, credit can be reclaimed."
        ),
    }
