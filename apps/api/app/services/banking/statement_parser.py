"""
Banking Statement Parser Service
─────────────────────────────────
Pipeline:  PDF → LlamaParse (text extraction) → OpenAI GPT-4o (JSON structuring) → DB
"""

import json
import logging
import tempfile
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import openai

from app.core.config import settings

logger = logging.getLogger(__name__)


# ─── OpenAI Structuring Prompt ─────────────────────────
STRUCTURING_PROMPT = """You are a banking data extraction AI specialising in Indian bank statements.

Given the raw text extracted from a bank statement PDF, extract ALL transaction rows into a strict JSON object.

**Rules:**
1. Extract EVERY transaction — do not skip any row.
2. Dates must be ISO format: "YYYY-MM-DD"
3. Amount fields must be plain numbers (no commas, no ₹ symbol) or null.
4. If a column is Withdrawal/Debit, put the value in "debit". If Deposit/Credit, put in "credit". The other field should be null.
5. "balance" is the running balance after the transaction.
6. "category" — auto-classify each transaction into one of:
   Salary, Rent, Utilities, GST Payment, TDS, Bank Charges, Interest Income, Interest Paid,
   Loan EMI, Insurance, Vendor Payment, Client Receipt, Transfer, Cash Withdrawal,
   Cash Deposit, UPI, Dividend, Refund, Government, Other
7. "party_name" — extract the counterparty name from the description/narration.
8. "reference_no" — extract cheque number, UTR, NEFT ref, IMPS ref, UPI ref if present.

Return this exact JSON structure (no extra text):
{
    "bank_name": "string",
    "account_number": "string (last 4 digits or masked)",
    "period_start": "YYYY-MM-DD",
    "period_end": "YYYY-MM-DD",
    "opening_balance": number or null,
    "closing_balance": number or null,
    "transactions": [
        {
            "date": "YYYY-MM-DD",
            "value_date": "YYYY-MM-DD or null",
            "description": "full narration text",
            "reference_no": "string or null",
            "debit": number or null,
            "credit": number or null,
            "balance": number or null,
            "category": "one of the categories above",
            "party_name": "string or null"
        }
    ]
}"""


async def extract_text_llamaparse(file_bytes: bytes, filename: str) -> str:
    """Step 1: Use LlamaParse to extract clean text from PDF.
    Prefers markdown output which captures tables correctly."""

    if not settings.LLAMA_CLOUD_API_KEY:
        raise ValueError("LLAMA_CLOUD_API_KEY not configured")

    try:
        from llama_cloud import AsyncLlamaCloud
    except ImportError:
        raise ImportError("llama_cloud package not installed. Run: pip install llama_cloud>=1.0")

    client = AsyncLlamaCloud(api_key=settings.LLAMA_CLOUD_API_KEY)

    # Write to temp file for upload
    with tempfile.NamedTemporaryFile(suffix=f"_{filename}", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    logger.info(f"Uploading {filename} ({len(file_bytes)} bytes) to LlamaParse...")

    try:
        file_obj = await client.files.create(file=tmp_path, purpose="parse")

        result = await client.parsing.parse(
            file_id=file_obj.id,
            tier="agentic",
            version="latest",
            output_options={
                "markdown": {
                    "tables": {
                        "output_tables_as_markdown": True,
                    },
                },
            },
            processing_options={
                "ocr_parameters": {
                    "languages": ["en"]
                },
            },
            expand=["text", "markdown", "items"],
        )

        # Prefer MARKDOWN (captures tables correctly) over plain text
        pages_text = []

        # 1) Try markdown first — best for bank statements with tables
        if result.markdown and result.markdown.pages:
            for page in result.markdown.pages:
                pages_text.append(page.markdown or "")
            logger.info(f"Using MARKDOWN mode: {len(pages_text)} pages")

        # 2) Fallback: try items for structured tables
        if not pages_text or sum(len(p) for p in pages_text) < 500:
            try:
                if result.items and result.items.pages:
                    items_text = []
                    for page in result.items.pages:
                        for item in page.items:
                            if hasattr(item, 'rows') and item.rows:
                                # Structured table — convert rows to text
                                for row in item.rows:
                                    items_text.append(" | ".join(str(c) for c in row))
                            elif hasattr(item, 'text'):
                                items_text.append(item.text or "")
                    if items_text:
                        pages_text = ["\n".join(items_text)]
                        logger.info(f"Using ITEMS mode: {len(items_text)} items")
            except Exception as e:
                logger.warning(f"Items extraction failed, continuing: {e}")

        # 3) Last resort: plain text
        if not pages_text or sum(len(p) for p in pages_text) < 500:
            if result.text and result.text.pages:
                pages_text = [page.text or "" for page in result.text.pages]
                logger.info(f"Using TEXT mode (fallback): {len(pages_text)} pages")

        full_text = "\n\n--- PAGE BREAK ---\n\n".join(pages_text)
        logger.info(f"LlamaParse extracted {len(full_text)} chars from {len(pages_text)} pages")
        return full_text

    except Exception as e:
        logger.error(f"LlamaParse extraction failed: {e}")
        raise


async def structure_with_openai(extracted_text: str) -> Dict[str, Any]:
    """Step 2: Use OpenAI GPT-4o to structure the extracted text into JSON."""

    if not settings.OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY not configured")

    client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)

    logger.info(f"Sending {len(extracted_text)} chars to OpenAI for structuring...")

    try:
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": STRUCTURING_PROMPT},
                {"role": "user", "content": extracted_text},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=16000,
        )

        content = response.choices[0].message.content
        structured = json.loads(content)

        txn_count = len(structured.get("transactions", []))
        logger.info(f"OpenAI extracted {txn_count} transactions")
        return structured

    except json.JSONDecodeError as e:
        logger.error(f"OpenAI returned invalid JSON: {e}")
        raise ValueError(f"Failed to parse AI response as JSON: {e}")
    except Exception as e:
        logger.error(f"OpenAI structuring failed: {e}")
        raise


def safe_date(val: Any) -> Optional[date]:
    """Parse a date string safely."""
    if not val:
        return None
    try:
        if isinstance(val, date):
            return val
        return datetime.strptime(str(val), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def safe_decimal(val: Any) -> Optional[Decimal]:
    """Parse a number into Decimal safely."""
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None
