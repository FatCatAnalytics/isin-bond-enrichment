"""
FastAPI backend for ISIN Bond Enrichment Pipeline.
Serves the dashboard and streams pipeline progress via WebSocket.

Usage:
    pip install -r requirements.txt
    python server.py
    # Open http://localhost:8000
"""

import asyncio
import time
import uuid
import io
import urllib3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from bond_enhancement import (
    chunk_list,
    lookup_isins_openfigi,
    process_openfigi_response,
    classify_market,
    HAS_WIN32,
)

# Suppress SSL warnings from OpenFIGI verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# App Setup
# ============================================================================
app = FastAPI(title="ISIN Bond Enrichment")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BASE_DIR = Path(__file__).parent


# ============================================================================
# Session State
# ============================================================================
@dataclass
class Session:
    isins: List[str]
    filename: str
    openfigi_results: Optional[List[Dict]] = None
    ciq_results: Optional[pd.DataFrame] = None
    final_df: Optional[pd.DataFrame] = None


sessions: Dict[str, Session] = {}


# ============================================================================
# Routes
# ============================================================================
@app.get("/")
async def serve_dashboard():
    return FileResponse(BASE_DIR / "dashboard.html")


@app.post("/api/upload")
async def upload_file(file: UploadFile, sheet_name: str = Query("Bank AG")):
    """Parse ISINs from uploaded Excel/CSV file."""
    content = await file.read()
    buf = io.BytesIO(content)

    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(buf)
        else:
            try:
                df = pd.read_excel(buf, sheet_name=sheet_name, engine="openpyxl")
            except ValueError:
                buf.seek(0)
                df = pd.read_excel(buf, sheet_name=0, engine="openpyxl")
                sheet_name = "Sheet1"
    except Exception as e:
        return {"error": f"Failed to parse file: {str(e)}"}

    # Find ISIN column
    isin_col = None
    for col in df.columns:
        if "ISIN" in str(col).upper():
            isin_col = col
            break
    if isin_col is None:
        isin_col = df.columns[0]

    isins = df[isin_col].dropna().astype(str).str.strip().tolist()
    isins = [i for i in isins if len(i) >= 10]

    session_id = str(uuid.uuid4())[:8]
    sessions[session_id] = Session(isins=isins, filename=file.filename)

    return {
        "session_id": session_id,
        "isins": isins,
        "count": len(isins),
        "filename": file.filename,
        "sheet_name": sheet_name,
    }


@app.get("/api/export/{session_id}")
async def export_results(session_id: str):
    """Download final merged results as Excel."""
    session = sessions.get(session_id)
    if not session or session.final_df is None:
        return {"error": "No results available"}

    buf = io.BytesIO()
    session.final_df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=enriched_bonds.xlsx"},
    )


# ============================================================================
# WebSocket Pipeline
# ============================================================================
@app.websocket("/ws/pipeline/{session_id}")
async def pipeline_ws(websocket: WebSocket, session_id: str):
    await websocket.accept()

    session = sessions.get(session_id)
    if not session:
        await websocket.send_json({"event": "error", "message": "Invalid session"})
        await websocket.close()
        return

    try:
        # Wait for start command
        msg = await websocket.receive_json()
        if msg.get("action") != "start":
            await websocket.send_json({"event": "error", "message": "Expected start action"})
            return

        api_key = msg.get("api_key")

        # ── Phase 1: OpenFIGI ──
        await run_openfigi_phase(websocket, session, api_key)

        # ── Phase 2: Capital IQ ──
        await run_capiq_phase(websocket, session)

        # ── Phase 3: Merge ──
        await run_merge_phase(websocket, session)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"event": "error", "message": str(e)})
        except Exception:
            pass


async def run_openfigi_phase(websocket: WebSocket, session: Session, api_key: Optional[str]):
    """Run OpenFIGI lookup, streaming batch progress."""
    isins = session.isins
    batch_size = 100 if api_key else 10
    delay = 2.5 if api_key else 10
    batches = list(chunk_list(isins, batch_size))
    total_batches = len(batches)

    await websocket.send_json({
        "event": "openfigi_start",
        "total_isins": len(isins),
        "batch_size": batch_size,
        "total_batches": total_batches,
        "has_api_key": bool(api_key),
    })

    all_results = []
    start_time = time.time()

    for i, batch in enumerate(batches):
        # Run blocking API call in thread
        response = await asyncio.to_thread(lookup_isins_openfigi, batch, api_key)

        batch_results = []
        if response:
            for j, result in enumerate(response):
                batch_results.append(process_openfigi_response(batch[j], result))
        else:
            for isin in batch:
                batch_results.append({
                    "ISIN": isin, "Name": "API call failed",
                    "Ticker": "", "Security_Type": "", "Security_Type2": "",
                    "Market_Sector": "", "Exchange_Code": "", "FIGI": "",
                    "Composite_FIGI": "", "Security_Description": "", "Num_Matches": 0,
                    "Asset_Class": "", "Market": classify_market(isin),
                })

        all_results.extend(batch_results)
        elapsed = time.time() - start_time
        pct = round(((i + 1) / total_batches) * 100)

        eta = 0
        if i > 0:
            eta = round((elapsed / (i + 1)) * (total_batches - i - 1))

        await websocket.send_json({
            "event": "openfigi_batch",
            "batch": i + 1,
            "total_batches": total_batches,
            "progress_pct": pct,
            "elapsed_s": round(elapsed),
            "eta_s": eta,
            "results": batch_results,
        })

        # Rate limit delay between batches
        if i < total_batches - 1:
            await asyncio.sleep(delay)

    session.openfigi_results = all_results
    elapsed = round(time.time() - start_time)

    matched = sum(1 for r in all_results if r["Name"] and "ERROR" not in r["Name"]
                  and r["Name"] != "No match found" and r["Name"] != "API call failed")
    not_found = sum(1 for r in all_results if r.get("Name") == "No match found")
    api_errors = sum(1 for r in all_results if r.get("Name", "").startswith("ERROR")
                     or r.get("Name") == "API call failed")
    g10 = sum(1 for r in all_results if r.get("Market") == "G10")
    em = sum(1 for r in all_results if r.get("Market") == "EM")

    await websocket.send_json({
        "event": "openfigi_complete",
        "matched": matched,
        "not_found": not_found,
        "api_errors": api_errors,
        "errors": not_found + api_errors,
        "g10": g10,
        "em": em,
        "elapsed_s": elapsed,
        "total": len(all_results),
    })


async def run_capiq_phase(websocket: WebSocket, session: Session):
    """Handle Capital IQ step — prompt user and process if confirmed."""
    await websocket.send_json({
        "event": "capiq_prompt",
        "has_win32": HAS_WIN32,
    })

    # Wait for user response
    msg = await websocket.receive_json()

    if msg.get("action") == "capiq_skip":
        await websocket.send_json({"event": "capiq_skipped"})
        return

    if msg.get("action") == "capiq_confirm":
        if not HAS_WIN32:
            await websocket.send_json({
                "event": "capiq_skipped",
                "reason": "pywin32 not available on this platform",
            })
            return

        template_path = msg.get("template_path", "ISIN Template.xlsx")

        await websocket.send_json({"event": "capiq_populating", "total": len(session.isins)})

        try:
            # Run CIQ in thread (blocking COM calls)
            from bond_enhancement import run_capiq_enrichment

            ciq_output = str(BASE_DIR / "ciq_results.xlsx")
            ciq_df = await asyncio.to_thread(
                run_capiq_enrichment, session.isins, template_path, ciq_output, False
            )
            session.ciq_results = ciq_df

            if ciq_df is not None:
                await websocket.send_json({
                    "event": "capiq_complete",
                    "rows": len(ciq_df),
                    "columns": len(ciq_df.columns),
                    "errors": 0,
                })
            else:
                await websocket.send_json({
                    "event": "capiq_skipped",
                    "reason": "CIQ enrichment returned no results",
                })
        except Exception as e:
            import traceback
            traceback.print_exc()
            await websocket.send_json({
                "event": "capiq_error",
                "message": str(e),
            })


async def run_merge_phase(websocket: WebSocket, session: Session):
    """Merge OpenFIGI + CIQ results and send final data."""
    openfigi_df = pd.DataFrame(session.openfigi_results)
    final_df = openfigi_df.copy()

    has_ciq = False

    if session.ciq_results is not None and len(session.ciq_results) > 0:
        ciq_df = session.ciq_results.copy()
        has_ciq = True

        # Find ISIN column in CIQ
        ciq_isin_col = None
        for col in ciq_df.columns:
            if "ISIN" in str(col).upper():
                ciq_isin_col = col
                break
        if ciq_isin_col is None:
            ciq_isin_col = ciq_df.columns[0]

        # Rename conflicting columns
        ciq_rename = {}
        for col in ciq_df.columns:
            if col != ciq_isin_col and col in final_df.columns:
                ciq_rename[col] = f"CIQ_{col}"
        ciq_df = ciq_df.rename(columns=ciq_rename)

        # Merge
        final_df = final_df.merge(ciq_df, left_on="ISIN", right_on=ciq_isin_col, how="left")
        if ciq_isin_col != "ISIN" and ciq_isin_col in final_df.columns:
            final_df = final_df.drop(columns=[ciq_isin_col])

    session.final_df = final_df

    # Convert to JSON-safe preview
    preview = final_df.head(100).fillna("").to_dict(orient="records")

    await websocket.send_json({
        "event": "merge_complete",
        "total_rows": len(final_df),
        "total_columns": len(final_df.columns),
        "columns": list(final_df.columns),
        "has_ciq": has_ciq,
        "preview": preview,
    })


# ============================================================================
# Run
# ============================================================================
if __name__ == "__main__":
    import uvicorn
    print("\n  ISIN Bond Enrichment Server")
    print("  http://localhost:8000\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
