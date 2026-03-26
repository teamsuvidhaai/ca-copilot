"""
Reconciliation API Endpoint
Handles synchronous reconciliation of GST files.
Accepts two files, runs matching logic, returns JSON results + downloadable report.
"""
from typing import Any
from uuid import uuid4
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api import deps
from app.models.models import User
from app.models.job import Job, JobStatus, JobType, JobEvent
from app.services.storage import storage_service
from app.services.gst.reconciliation import (
    load_reconciliation_file, identify_columns, rename_to_standard,
    match_data, drop_total_rows, separate_blocked_itc, filter_voucher_types,
    get_source_labels, generate_excel_report,
)
from app.services.drive_saver import save_report_to_drive_async

import pandas as pd
import io
import os
import traceback
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/run")
async def run_reconciliation(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    job_type: str = Form("gstr2b_vs_pr"),
    client_id: str = Form(None),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Run reconciliation synchronously on two uploaded files.
    Returns JSON summary + saves Excel report for download.
    """
    try:
        # Read file bytes
        bytes1 = await file1.read()
        bytes2 = await file2.read()
        fn1 = file1.filename or "file1.xlsx"
        fn2 = file2.filename or "file2.xlsx"

        # ── Parse both files ──
        df1 = load_reconciliation_file(bytes1, fn1)
        df2 = load_reconciliation_file(bytes2, fn2)

        logger.info(f"━━━ Loaded: {fn1} ({len(df1)} rows, cols: {list(df1.columns)[:8]})")
        logger.info(f"━━━ Loaded: {fn2} ({len(df2)} rows, cols: {list(df2.columns)[:8]})")

        cols1 = identify_columns(df1)
        cols2 = identify_columns(df2)

        df1_std = rename_to_standard(df1, cols1)
        df2_std = rename_to_standard(df2, cols2)

        # ── Clean: Drop total rows ──
        df1_std = drop_total_rows(df1_std, "GSTR-2B")
        df2_std = drop_total_rows(df2_std, "PR")

        # ── Filter voucher types (only Purchase & Debit Note from Tally) ──
        df2_std = filter_voucher_types(df2_std, "PR")

        # ── Separate blocked ITC ──
        df1_std, df1_blocked = separate_blocked_itc(df1_std, "GSTR-2B")
        df2_std, df2_blocked = separate_blocked_itc(df2_std, "PR")

        logger.info(f"━━━ After cleaning → GSTR-2B: {len(df1_std)} rows, PR: {len(df2_std)} rows")

        # ── Run matching ──
        result_df = match_data(df1_std, df2_std, job_type)


        # ── Source labels ──
        source1_label, source2_label = get_source_labels(job_type)

        # ── Build summary stats ──
        total = len(result_df)
        matched = int((result_df['Status'] == 'MATCHED').sum())
        mismatched = int(result_df['Status'].str.contains('MISMATCH', na=False).sum())
        missing_in_s2 = int(result_df['Status'].str.contains(f'MISSING IN {source2_label.upper()}', na=False).sum())
        missing_in_s1 = int(result_df['Status'].str.contains(f'MISSING IN {source1_label.upper()}', na=False).sum())

        # Tax component summaries (only from matched/compared rows, excluding blocked ITC)
        tax_summary = {}
        for comp in ['taxable', 'igst', 'cgst', 'sgst', 'cess']:
            src1_col = f'{comp}_src1'
            src2_col = f'{comp}_src2'
            diff_col = f'Diff_{comp.capitalize()}'
            if src1_col in result_df.columns and src2_col in result_df.columns:
                s1_total = float(result_df[src1_col].fillna(0).sum())
                s2_total = float(result_df[src2_col].fillna(0).sum())
                diff_total = float(result_df[diff_col].fillna(0).sum()) if diff_col in result_df.columns else s1_total - s2_total
                tax_summary[comp] = {
                    'source1': round(s1_total, 2),
                    'source2': round(s2_total, 2),
                    'difference': round(diff_total, 2)
                }

        # Blocked ITC summary
        blocked_summary = None
        all_blocked = pd.concat([df1_blocked, df2_blocked], ignore_index=True) if (len(df1_blocked) + len(df2_blocked)) > 0 else pd.DataFrame()
        if len(all_blocked) > 0:
            blocked_summary = {
                'count': len(df1_blocked) + len(df2_blocked),
                'total_tax': round(sum(
                    float(all_blocked[c].fillna(0).sum()) for c in ['igst', 'cgst', 'sgst', 'cess'] if c in all_blocked.columns
                ), 2),
            }

        # Build row-level details (first 500 rows for frontend display)
        # Filter out internal columns
        skip_cols = {'_merge', 'inv_num_display_src1', 'inv_num_display_src2',
                     'itc_eligible_src1', 'itc_eligible_src2'}
        display_cols = ['Status'] + [c for c in result_df.columns if c != 'Status' and c not in skip_cols]

        rows_data = []
        for _, row in result_df.head(500).iterrows():
            row_dict = {}
            for col in display_cols:
                if col in result_df.columns:
                    val = row[col]
                    if pd.isna(val):
                        row_dict[col] = None
                    elif isinstance(val, (float, int)):
                        row_dict[col] = round(float(val), 2)
                    else:
                        row_dict[col] = str(val)
            rows_data.append(row_dict)

        # ── Generate + save Excel report ──
        report_bytes = generate_excel_report(
            result_df, df1_std, df2_std, df1_blocked, df2_blocked,
            source1_label, source2_label
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        client_folder = client_id if client_id else "General"
        report_name = job_type.upper().replace("_", "-")
        output_key = f"Reports/{current_user.firm_id}/{client_folder}/{report_name}_{timestamp}.xlsx"

        storage_service.upload_file(
            report_bytes, output_key,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Create completed job record
        from sqlalchemy.future import select
        from sqlalchemy.orm import selectinload

        job = Job(
            job_type=job_type,
            input_files=[fn1, fn2],
            output_files=[output_key],
            status=JobStatus.COMPLETED,
            created_by=current_user.id,
            firm_id=current_user.firm_id,
            client_id=client_id if client_id else None,
        )

        # Auto-save to Client Drive
        drive_fid = await save_report_to_drive_async(
            db=db,
            firm_id=current_user.firm_id,
            client_id=client_id if client_id else None,
            job_type=job_type,
            output_file_bytes=report_bytes,
            output_key=output_key,
        )
        if drive_fid:
            job.drive_file_id = drive_fid

        db.add(job)
        await db.commit()

        result = await db.execute(
            select(Job).options(selectinload(Job.events)).filter(Job.id == job.id)
        )
        job = result.scalars().first()

        # ── Low PR row count warning ──
        pr_count = len(df2_std)
        gstr2b_count = len(df1_std)
        low_pr_warning = None
        if gstr2b_count > 0 and pr_count < gstr2b_count * 0.3:
            low_pr_warning = (
                f"⚠️ Purchase Register has only {pr_count} rows vs {gstr2b_count} rows in GSTR-2B. "
                f"This may indicate the Tally export is incomplete. "
                f"Please re-export including Journal vouchers with GST entries."
            )
            logger.warning(f"  ⚠️ Low PR warning: {pr_count} PR rows vs {gstr2b_count} 2B rows")

        return JSONResponse(content={
            "success": True,
            "job_id": str(job.id),
            "summary": {
                "total_invoices": total,
                "matched": matched,
                "mismatched": mismatched,
                "missing_in_source2": missing_in_s2,
                "missing_in_source1": missing_in_s1,
                "match_rate": round(matched / total * 100, 1) if total > 0 else 0,
                "source1_label": source1_label,
                "source2_label": source2_label,
            },
            "tax_summary": tax_summary,
            "blocked_itc": blocked_summary,
            "low_pr_warning": low_pr_warning,
            "columns": display_cols,
            "rows": rows_data,
            "total_rows": total,
        })

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gstr1-vs-3b")
async def run_gstr1_vs_3b(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    client_id: str = Form(None),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """
    GSTR-1 vs GSTR-3B — Summary-level tax liability reconciliation.
    
    file1: GSTR-1 (.xlsx, .csv, .json)
    file2: GSTR-3B (.xlsx, .csv, .json)
    
    Returns variance per tax component, risk assessment, and recommended actions.
    """
    try:
        from app.services.gst.gstr1_vs_3b import reconcile_gstr1_vs_3b
        
        bytes1 = await file1.read()
        bytes2 = await file2.read()
        fn1 = file1.filename or "gstr1.xlsx"
        fn2 = file2.filename or "gstr3b.xlsx"
        
        result = reconcile_gstr1_vs_3b([bytes1, bytes2], [fn1, fn2])
        
        # Save Excel report
        report_bytes = result.pop('report_bytes')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        client_folder = client_id if client_id else "General"
        output_key = f"Reports/{current_user.firm_id}/{client_folder}/GSTR1-VS-3B_{timestamp}.xlsx"
        
        storage_service.upload_file(
            report_bytes, output_key,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        # Create job record
        from sqlalchemy.future import select
        from sqlalchemy.orm import selectinload
        
        job = Job(
            job_type="gstr1_vs_3b",
            input_files=[fn1, fn2],
            output_files=[output_key],
            status=JobStatus.COMPLETED,
            created_by=current_user.id,
            firm_id=current_user.firm_id,
            client_id=client_id if client_id else None,
        )

        # Auto-save to Client Drive
        drive_fid = await save_report_to_drive_async(
            db=db,
            firm_id=current_user.firm_id,
            client_id=client_id if client_id else None,
            job_type="gstr1_vs_3b",
            output_file_bytes=report_bytes,
            output_key=output_key,
        )
        if drive_fid:
            job.drive_file_id = drive_fid

        db.add(job)
        await db.commit()
        
        db_result = await db.execute(
            select(Job).options(selectinload(Job.events)).filter(Job.id == job.id)
        )
        job = db_result.scalars().first()
        
        return JSONResponse(content={
            "success": True,
            "job_id": str(job.id),
            **result,
        })
    
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gstr2b-vs-3b")
async def run_gstr2b_vs_3b(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    client_id: str = Form(None),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """
    GSTR-2B vs GSTR-3B — Summary-level ITC reconciliation.

    file1: GSTR-2B (.xlsx, .csv, .json, .pdf)
    file2: GSTR-3B (.xlsx, .csv, .json, .pdf)

    Returns ITC variance per component, risk assessment, and recommended actions.
    """
    try:
        from app.services.gst.gstr2b_vs_3b import reconcile_gstr2b_vs_3b

        bytes1 = await file1.read()
        bytes2 = await file2.read()
        fn1 = file1.filename or "gstr2b.xlsx"
        fn2 = file2.filename or "gstr3b.xlsx"

        result = reconcile_gstr2b_vs_3b([bytes1, bytes2], [fn1, fn2])

        # Save Excel report
        report_bytes = result.pop('report_bytes')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        client_folder = client_id if client_id else "General"
        output_key = f"Reports/{current_user.firm_id}/{client_folder}/GSTR2B-VS-3B_{timestamp}.xlsx"

        storage_service.upload_file(
            report_bytes, output_key,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Create job record
        from sqlalchemy.future import select
        from sqlalchemy.orm import selectinload

        job = Job(
            job_type="gstr2b_vs_3b",
            input_files=[fn1, fn2],
            output_files=[output_key],
            status=JobStatus.COMPLETED,
            created_by=current_user.id,
            firm_id=current_user.firm_id,
            client_id=client_id if client_id else None,
        )

        # Auto-save to Client Drive
        drive_fid = await save_report_to_drive_async(
            db=db,
            firm_id=current_user.firm_id,
            client_id=client_id if client_id else None,
            job_type="gstr2b_vs_3b",
            output_file_bytes=report_bytes,
            output_key=output_key,
        )
        if drive_fid:
            job.drive_file_id = drive_fid

        db.add(job)
        await db.commit()

        db_result = await db.execute(
            select(Job).options(selectinload(Job.events)).filter(Job.id == job.id)
        )
        job = db_result.scalars().first()

        return JSONResponse(content={
            "success": True,
            "job_id": str(job.id),
            **result,
        })

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/calculate-refund")
async def calculate_gst_refund(
    request_data: dict,
    current_user: User = Depends(deps.get_current_active_user),
):
    """
    GST Refund Calculator — compute maximum admissible refund.

    Accepts JSON body with refund_type and corresponding input values.
    Returns computed refund amount, formula breakdown, and warnings.

    Supported refund_type values:
      - export_goods_lut   → Rule 89(4)
      - export_service_lut → Rule 89(4)
      - deemed_export      → Rule 89(4)
      - inverted_duty      → Rule 89(5)
      - export_igst        → Rule 96
      - excess_cash        → Direct
    """
    try:
        from app.services.gst.refund_calculator import calculate_refund

        if not request_data.get("refund_type"):
            raise HTTPException(status_code=400, detail="refund_type is required")

        result = calculate_refund(request_data)

        if result.get("error"):
            raise HTTPException(status_code=400, detail=result["error"])

        logger.info(f"Refund calc: type={request_data['refund_type']}, result=₹{result['max_refund']:,.2f}")
        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/calculate-refund-from-files")
async def calculate_refund_from_files(
    refund_type: str = Form(...),
    period: str = Form(None),
    files: list[UploadFile] = File(...),
    current_user: User = Depends(deps.get_current_active_user),
):
    """
    GST Refund Calculator — file-based.

    Accepts uploaded GSTR-3B, GSTR-1, Purchase Register, etc.
    Extracts relevant values using AI parsing, then computes the refund.

    Returns the same result as /calculate-refund plus extraction metadata.
    """
    try:
        from app.services.gst.refund_file_extractor import extract_refund_values
        from app.services.gst.refund_calculator import calculate_refund

        if not refund_type:
            raise HTTPException(status_code=400, detail="refund_type is required")

        if not files or len(files) == 0:
            raise HTTPException(status_code=400, detail="At least one file is required")

        if len(files) > 5:
            raise HTTPException(status_code=400, detail="Maximum 5 files allowed")

        # Read all files
        file_bytes_list = []
        filenames = []
        for f in files:
            content = await f.read()
            if len(content) > 10 * 1024 * 1024:  # 10MB limit per file
                raise HTTPException(
                    status_code=400,
                    detail=f"File '{f.filename}' exceeds 10MB limit"
                )
            file_bytes_list.append(content)
            filenames.append(f.filename or "unknown.xlsx")

        logger.info(f"Refund file calc: type={refund_type}, files={filenames}")

        # Extract values from files
        extracted_data = extract_refund_values(file_bytes_list, filenames, refund_type)

        # Pull out metadata before passing to calculator
        extraction_notes = extracted_data.pop("_extraction_notes", [])
        file_types_found = extracted_data.pop("_file_types_found", [])

        # Calculate refund using extracted values
        result = calculate_refund(extracted_data)

        if result.get("error"):
            raise HTTPException(status_code=400, detail=result["error"])

        # Add extraction metadata to result
        result["extraction"] = {
            "files_processed": filenames,
            "file_types_detected": file_types_found,
            "notes": extraction_notes,
            "extracted_values": {
                k: v for k, v in extracted_data.items()
                if k != "refund_type" and isinstance(v, (int, float))
            },
        }

        logger.info(
            f"Refund file calc: type={refund_type}, "
            f"files={file_types_found}, result=₹{result['max_refund']:,.2f}"
        )
        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
