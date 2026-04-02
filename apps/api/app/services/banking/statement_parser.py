"""
Banking Statement Parser Service
─────────────────────────────────
Pipeline:  PDF → LlamaParse (text extraction) → OpenAI GPT-4o (JSON structuring) → DB
"""

import json
import logging
import re
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


def _repair_json(raw: str) -> dict:
    """Attempt to repair malformed JSON from AI responses.
    Handles: trailing commas, truncated output, unclosed brackets."""

    # Strip markdown code fences if present
    raw = raw.strip()
    if raw.startswith("```"):
        raw = re.sub(r'^```(?:json)?\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)

    # Try direct parse first
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Fix 1: Remove trailing commas before ] or }
    fixed = re.sub(r',\s*([\]}])', r'\1', raw)
    try:
        return json.loads(fixed)
    except json.JSONDecodeError:
        pass

    # Fix 2: Truncated response — close open brackets/braces
    # Count unclosed brackets
    open_braces = fixed.count('{') - fixed.count('}')
    open_brackets = fixed.count('[') - fixed.count(']')

    # If truncated mid-object inside transactions array
    if open_braces > 0 or open_brackets > 0:
        # Remove the last incomplete object (after the last complete })
        last_complete = fixed.rfind('}')
        if last_complete > 0:
            # Find the last comma before incomplete part
            after = fixed[last_complete + 1:].strip()
            if after.startswith(','):
                fixed = fixed[:last_complete + 1]
            elif after and not after[0] in ']}':
                fixed = fixed[:last_complete + 1]

        # Close remaining brackets
        open_braces = fixed.count('{') - fixed.count('}')
        open_brackets = fixed.count('[') - fixed.count(']')
        fixed += ']' * max(0, open_brackets)
        fixed += '}' * max(0, open_braces)

    # Final trailing comma cleanup
    fixed = re.sub(r',\s*([\]}])', r'\1', fixed)

    try:
        return json.loads(fixed)
    except json.JSONDecodeError as e:
        raise ValueError(f"Could not repair JSON: {e}")


PAGES_PER_CHUNK = 5  # Process 5 pages at a time


async def _call_openai_chunk(client, text_chunk: str, is_first: bool) -> Dict[str, Any]:
    """Send a single chunk to OpenAI and return parsed JSON."""
    prompt = STRUCTURING_PROMPT
    if not is_first:
        prompt += "\n\nNOTE: This is a CONTINUATION of a multi-page statement. Extract ONLY the transactions from this section. For bank_name, account_number, period_start, period_end, opening_balance, closing_balance — set them to null (they were captured from the first chunk)."

    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": text_chunk},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
        max_tokens=16000,
    )

    content = response.choices[0].message.content
    finish_reason = response.choices[0].finish_reason
    logger.info(f"  Chunk response: {len(content)} chars, finish_reason={finish_reason}")

    return _repair_json(content)


async def structure_with_openai(extracted_text: str) -> Dict[str, Any]:
    """Step 2: Use OpenAI GPT-4o to structure the extracted text into JSON.
    For large statements, splits into chunks of 5 pages and merges results."""

    if not settings.OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY not configured")

    client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)

    # Split text into pages
    pages = extracted_text.split("--- PAGE BREAK ---")
    page_count = len(pages)
    logger.info(f"Statement has {page_count} pages, {len(extracted_text)} chars total")

    # Small statement (≤5 pages) — single call
    if page_count <= PAGES_PER_CHUNK:
        logger.info("Small statement — single OpenAI call")
        try:
            result = await _call_openai_chunk(client, extracted_text, is_first=True)
            txn_count = len(result.get("transactions", []))
            logger.info(f"✅ Extracted {txn_count} transactions in single call")
            return result
        except Exception as e:
            logger.error(f"OpenAI structuring failed: {e}")
            raise

    # Large statement — process in chunks of 5 pages
    chunks = []
    for i in range(0, page_count, PAGES_PER_CHUNK):
        chunk_pages = pages[i:i + PAGES_PER_CHUNK]
        chunks.append("\n\n--- PAGE BREAK ---\n\n".join(chunk_pages))

    logger.info(f"Large statement — splitting into {len(chunks)} chunks of {PAGES_PER_CHUNK} pages each")

    # Process first chunk (gets metadata + transactions)
    try:
        first_result = await _call_openai_chunk(client, chunks[0], is_first=True)
    except Exception as e:
        logger.error(f"First chunk failed: {e}")
        raise

    all_transactions = first_result.get("transactions", [])
    logger.info(f"  Chunk 1/{len(chunks)}: {len(all_transactions)} transactions")

    # Process remaining chunks IN PARALLEL (transactions only)
    import asyncio
    async def _process_chunk(idx, chunk_text):
        try:
            chunk_result = await _call_openai_chunk(client, chunk_text, is_first=False)
            chunk_txns = chunk_result.get("transactions", [])
            logger.info(f"  Chunk {idx}/{len(chunks)}: {len(chunk_txns)} transactions")
            return chunk_txns
        except Exception as e:
            logger.error(f"Chunk {idx} failed: {e}. Skipping.")
            return []

    if len(chunks) > 1:
        tasks = [_process_chunk(idx, ct) for idx, ct in enumerate(chunks[1:], start=2)]
        results = await asyncio.gather(*tasks)
        for txns in results:
            all_transactions.extend(txns)

    # Merge: use first chunk's metadata + all transactions
    first_result["transactions"] = all_transactions
    logger.info(f"✅ Total: {len(all_transactions)} transactions from {len(chunks)} chunks")

    return first_result


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
