"""
ISIN Bond Data Enrichment Pipeline
===================================
Combines OpenFIGI API lookup with Capital IQ Excel add-in enrichment.

Workflow:
    1. Run OpenFIGI lookup for basic bond data + classifications
    2. Prompt user to open Excel with CIQ template (add-in needs to load)
    3. Populate ISINs into CIQ template and wait for formulas
    4. Merge CIQ results with OpenFIGI data
    5. Apply Asset_Class and Market classifications based on merged data
    6. Output final enriched dataset

Usage:
    python bond_enrichment.py input.xlsx output.xlsx --template ciq_template.xlsx

Requirements:
    - Python packages: pip install requests pandas openpyxl pywin32
    - Capital IQ Excel Add-in installed
    - OpenFIGI API key (optional, for faster processing)
"""

import requests
import json
import csv
import time
import argparse
import os
import sys
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd

# Windows-specific imports (for CIQ integration)
try:
    import win32com.client as win32
    import pythoncom

    HAS_WIN32 = True
except ImportError:
    HAS_WIN32 = False
    print("Note: pywin32 not installed - Capital IQ integration disabled")
    print("      Install with: pip install pywin32")

# ============================================================================
# CONFIGURATION
# ============================================================================

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# G10 countries (by ISIN country prefix AND Capital IQ Country of Issuance)
G10_COUNTRIES = {
    'US', 'GB', 'DE', 'FR', 'IT', 'JP', 'CA', 'BE', 'NL', 'SE', 'CH',
    'AU', 'AT', 'ES', 'IE', 'FI', 'PT', 'DK', 'NO', 'NZ', 'LU', 'XS',
    # Add full country names for Capital IQ matching
    'USA', 'UNITED KINGDOM', 'GERMANY', 'FRANCE', 'ITALY', 'JAPAN',
    'CANADA', 'BELGIUM', 'NETHERLANDS', 'SWEDEN', 'SWITZERLAND', 'AUSTRALIA',
    'AUSTRIA', 'SPAIN', 'IRELAND', 'FINLAND', 'PORTUGAL', 'DENMARK', 'NORWAY',
    'NEW ZEALAND', 'LUXEMBOURG', 'GREECE'
}

# Market sectors that indicate government/sovereign bonds
GOVT_SECTORS = {'Govt', 'Muni'}

# Market sectors that indicate corporate bonds (Credit)
CORP_SECTORS = {'Corp'}

# Security types that typically indicate government bonds
GOVT_SECURITY_TYPES = {'DOMESTIC', 'Govt', 'T-Bill', 'Treasury', 'Sovereign'}

# Corporate security-type keywords that should NOT be treated as government
# even if they contain a government indicator (e.g. "DOMESTIC MTN")
CORP_SECURITY_KEYWORDS = {'MTN', 'MEDIUM TERM NOTE', 'CORPORATE', 'DEBENTURE',
                          'CONVERTIBLE', 'HIGH YIELD', 'COVERED'}


# ============================================================================
# CLASSIFICATION FUNCTIONS
# ============================================================================

def get_country_from_isin(isin: str) -> str:
    """Extract 2-letter country code from ISIN."""
    if isin and len(isin) >= 2:
        return isin[:2].upper()
    return ''


def classify_market_from_ciq(country_of_issuance: str, isin: str = '') -> str:
    """
    Classify market as G10 or EM using Capital IQ Country of Issuance first,
    then fallback to ISIN country prefix.
    """
    # Primary: Use Capital IQ Country of Issuance
    if country_of_issuance and not pd.isna(country_of_issuance):
        country_upper = str(country_of_issuance).upper().strip()
        if country_upper in G10_COUNTRIES:
            return 'G10'
        else:
            return 'EM'

    # Fallback: Use ISIN country prefix
    if isin:
        country_code = get_country_from_isin(isin)
        return classify_market(country_code)

    return 'EM'  # Default


def classify_market(isin: str) -> str:
    """Classify as G10 or EM based on ISIN country code (fallback method)."""
    country = get_country_from_isin(isin)
    return 'G10' if country in G10_COUNTRIES else 'EM'


def classify_asset_class_ciq(security_type: str, market_sector: str = '', security_type2: str = '') -> str:
    """
    Classify as Rate or Credit based on Capital IQ Security Type values.

    Capital IQ Security Type Classification:
    Rate (Government bonds):
      - Sovereign Bond
      - Supranational Bond
      - Supranational Note

    Credit (Corporate bonds):
      - Corporate Note
      - Corporate Bond
      - Corporate Money Market Instrument
      - Corporate Convertible

    Args:
        security_type: Capital IQ Security Type (primary classification source)
        market_sector: OpenFIGI Market Sector (fallback)
        security_type2: OpenFIGI Security Type 2 (fallback)

    Returns:
        'Rate' for government/sovereign bonds, 'Credit' for corporate bonds
    """
    if not security_type or pd.isna(security_type):
        # Fallback to OpenFIGI data if no CIQ Security Type
        if market_sector in GOVT_SECTORS:
            return 'Rate'
        elif market_sector in CORP_SECTORS:
            return 'Credit'
        else:
            return 'Credit'  # Default

    security_type_clean = str(security_type).strip().upper()

    # Rate (Government/Sovereign) Classification
    rate_types = {
        'SOVEREIGN BOND',
        'SUPRANATIONAL BOND',
        'SUPRANATIONAL NOTE'
    }

    if security_type_clean in rate_types:
        return 'Rate'

    # Credit (Corporate) Classification
    credit_types = {
        'CORPORATE NOTE',
        'CORPORATE BOND',
        'CORPORATE MONEY MARKET INSTRUMENT',
        'CORPORATE CONVERTIBLE'
    }

    if security_type_clean in credit_types:
        return 'Credit'

    # If exact match not found, check for key words
    if 'SOVEREIGN' in security_type_clean or 'SUPRANATIONAL' in security_type_clean:
        return 'Rate'
    elif 'CORPORATE' in security_type_clean:
        return 'Credit'

    # Final fallback to OpenFIGI data
    if market_sector in GOVT_SECTORS:
        return 'Rate'
    elif market_sector in CORP_SECTORS:
        return 'Credit'

    return 'Credit'  # Default to Credit if uncertain


def classify_asset_class(market_sector: str, security_type: str, security_type2: str) -> str:
    """
    Classify as Rate or Credit based on market sector and security type.
    Rate  = Government bonds (sovereign, treasury, muni)
    Credit = Corporate bonds, structured products, etc.

    Priority order:
      1. Market sector 'Govt' / 'Muni'  → Rate
      2. Market sector 'Corp'           → Credit  (even if securityType
         contains a word like DOMESTIC, e.g. "DOMESTIC MTN")
      3. Check securityType / securityType2 for government indicators,
         but only when no corporate keyword is present in the same field
      4. Default                         → Credit
    """
    # 1. Definitive government sectors
    if market_sector in GOVT_SECTORS:
        return 'Rate'

    # 2. Definitive corporate sector — skip security-type heuristics
    if market_sector in CORP_SECTORS:
        return 'Credit'

    # 3. Heuristic: look for government indicators in security types,
    #    but exclude when a corporate keyword is also present
    for sec_type in [security_type, security_type2]:
        if sec_type:
            sec_type_upper = sec_type.upper()

            # If the security type contains a corporate keyword, treat as Credit
            if any(kw in sec_type_upper for kw in CORP_SECURITY_KEYWORDS):
                return 'Credit'

            for govt_indicator in GOVT_SECURITY_TYPES:
                if govt_indicator.upper() in sec_type_upper:
                    return 'Rate'

    return 'Credit'


def apply_classifications(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply Asset_Class and Market classifications using Capital IQ data primarily.
    """
    print("Applying classifications using Capital IQ Security Type...")

    # Check if we have Capital IQ Security Type column
    security_type_col = 'Security Type'  # Capital IQ column
    has_ciq_security_type = security_type_col in df.columns

    if has_ciq_security_type:
        print(f"✓ Found Capital IQ '{security_type_col}' column")
        # Show the unique Security Type values
        unique_types = df[security_type_col].dropna().unique()
        print(f"  Unique Security Types found: {list(unique_types)}")
    else:
        print(f"⚠ Capital IQ '{security_type_col}' column not found, using OpenFIGI fallback")

    # Apply Asset_Class classification
    df['Asset_Class'] = df.apply(
        lambda row: classify_asset_class_ciq(
            row.get('Security Type', ''),  # Capital IQ Security Type (primary)
            row.get('Market_Sector', ''),  # OpenFIGI Market Sector (fallback)
            row.get('Security_Type2', '')  # OpenFIGI Security Type 2 (fallback)
        ),
        axis=1
    )

    # Apply Market classification
    df['Market'] = df.apply(
        lambda row: classify_market_from_ciq(
            row.get('Country of Issuance', ''),  # Capital IQ Country (primary)
            row.get('ISIN', '')  # ISIN country prefix (fallback)
        ),
        axis=1
    )

    # Count and report classifications
    rate_count = sum(df['Asset_Class'] == 'Rate')
    credit_count = sum(df['Asset_Class'] == 'Credit')
    g10_count = sum(df['Market'] == 'G10')
    em_count = sum(df['Market'] == 'EM')

    print(f"\n✓ Final Classifications:")
    print(f"  Asset Class: Rate={rate_count}, Credit={credit_count}")
    print(f"  Market: G10={g10_count}, EM={em_count}")

    # Show breakdown by Security Type if available
    if has_ciq_security_type:
        print(f"\n📊 Asset Class by Security Type:")
        breakdown = df.groupby(['Security Type', 'Asset_Class']).size().unstack(fill_value=0)
        for sec_type in breakdown.index:
            rate_cnt = breakdown.loc[sec_type, 'Rate'] if 'Rate' in breakdown.columns else 0
            credit_cnt = breakdown.loc[sec_type, 'Credit'] if 'Credit' in breakdown.columns else 0
            print(f"  {sec_type}: Rate={rate_cnt}, Credit={credit_cnt}")

    return df


# ============================================================================
# OPENFIGI LOOKUP
# ============================================================================

def chunk_list(lst: List, chunk_size: int):
    """Split list into chunks of specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def lookup_isins_openfigi(isins: List[str], api_key: Optional[str] = None) -> Optional[List[Dict]]:
    """Query OpenFIGI API for a batch of ISINs."""
    jobs = [{"idType": "ID_ISIN", "idValue": isin} for isin in isins]

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    try:
        response = requests.post(OPENFIGI_URL, headers=headers, json=jobs, timeout=30, verify=False)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print("Rate limit hit. Waiting 60 seconds...")
            time.sleep(60)
            return lookup_isins_openfigi(isins, api_key)
        else:
            print(f"API Error {response.status_code}: {response.text[:200]}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


def process_openfigi_response(isin: str, result: Dict) -> Dict:
    """Process a single ISIN response from OpenFIGI."""
    base_record = {
        'ISIN': isin,
        'Name': '',
        'Ticker': '',
        'Security_Type': '',
        'Security_Type2': '',
        'Market_Sector': '',
        'Exchange_Code': '',
        'FIGI': '',
        'Composite_FIGI': '',
        'Security_Description': '',
        'Num_Matches': 0,
        # Remove Asset_Class and Market - will be added later after CIQ merge
    }

    if 'data' in result and result['data']:
        match = result['data'][0]
        return {
            'ISIN': isin,
            'Name': match.get('name', ''),
            'Ticker': match.get('ticker', ''),
            'Security_Type': match.get('securityType', ''),
            'Security_Type2': match.get('securityType2', ''),
            'Market_Sector': match.get('marketSector', ''),
            'Exchange_Code': match.get('exchCode', ''),
            'FIGI': match.get('figi', ''),
            'Composite_FIGI': match.get('compositeFIGI', '') or '',
            'Security_Description': match.get('securityDescription', ''),
            'Num_Matches': len(result['data']),
            # Classifications will be applied after CIQ merge
        }
    elif 'error' in result:
        base_record['Name'] = f"ERROR: {result['error']}"
        return base_record
    else:
        base_record['Name'] = 'No match found'
        return base_record


def run_openfigi_lookup(isins: List[str], api_key: Optional[str] = None) -> List[Dict]:
    """Run OpenFIGI lookup for a list of ISINs."""

    print("\n" + "=" * 60)
    print("STEP 1: OpenFIGI Lookup")
    print("=" * 60)

    # Determine batch size and delay based on API key
    if api_key:
        batch_size = 100
        delay = 2.5
        print("Using API key - higher rate limits enabled")
    else:
        batch_size = 10
        delay = 10
        print("No API key - using conservative rate limits")

    results = []
    batches = list(chunk_list(isins, batch_size))
    total_batches = len(batches)
    start_time = time.time()

    for batch_num, batch in enumerate(batches):
        progress = (batch_num + 1) / total_batches * 100
        elapsed = time.time() - start_time

        if batch_num > 0:
            eta = (elapsed / batch_num) * (total_batches - batch_num)
            eta_str = f", ETA: {int(eta // 60)}m {int(eta % 60)}s"
        else:
            eta_str = ""

        print(f"Processing batch {batch_num + 1}/{total_batches} ({progress:.1f}%{eta_str})")

        response_data = lookup_isins_openfigi(batch, api_key)

        if response_data:
            for i, result in enumerate(response_data):
                results.append(process_openfigi_response(batch[i], result))
        else:
            for isin in batch:
                results.append({
                    'ISIN': isin, 'Name': 'API call failed',
                    'Ticker': '', 'Security_Type': '', 'Security_Type2': '',
                    'Market_Sector': '', 'Exchange_Code': '', 'FIGI': '',
                    'Composite_FIGI': '', 'Security_Description': '', 'Num_Matches': 0,
                })

        if batch_num < total_batches - 1:
            time.sleep(delay)

    # Summary
    elapsed = time.time() - start_time
    matched = sum(1 for r in results if r['Name'] and 'ERROR' not in r['Name']
                  and r['Name'] != 'No match found' and r['Name'] != 'API call failed')

    print(f"\nOpenFIGI Complete: {matched}/{len(results)} matched in {int(elapsed)}s")

    return results


# ============================================================================
# CAPITAL IQ INTEGRATION
# ============================================================================

def get_existing_excel():
    """Try to connect to an existing Excel instance."""
    try:
        excel = win32.GetActiveObject('Excel.Application')
        print("✓ Connected to existing Excel instance")
        return excel, False
    except:
        return None, False


def _is_cell_pending(val_text: str) -> bool:
    """Check if a cell value indicates a CIQ formula is still loading."""
    if not val_text:
        return False
    upper = val_text.upper().strip()
    return any(indicator in upper for indicator in (
        "#REQUESTING", "#GETTING", "#FETCHING", "#LOADING",
        "#CALCULATING", "#PENDING", "#WAIT",
    ))


def _col_letter(col_num: int) -> str:
    """Convert 1-based column number to Excel letter (1='A', 27='AA', etc.)."""
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _read_column_bulk(ws, col: int, start_row: int, end_row: int) -> list:
    """Read an entire column range at once using bulk COM read (single call)."""
    rng = ws.Range(ws.Cells(start_row, col), ws.Cells(end_row, col))
    # .Value on a range returns a tuple of tuples: ((val,), (val,), ...)
    vals = rng.Value
    if vals is None:
        return [''] * (end_row - start_row + 1)
    # Single cell returns a scalar, not a tuple
    if not isinstance(vals, tuple):
        return [str(vals) if vals else '']
    return [str(row[0]) if row[0] is not None else '' for row in vals]


def wait_for_ciq_formulas(ws, data_start_row, data_end_row, formula_cols,
                          timeout=600, check_interval=10):
    """
    Wait for ALL CIQ formulas to finish calculating across ALL rows and
    ALL formula columns.

    Uses BULK range reads instead of cell-by-cell access for ~100x speed-up.

    Args:
        ws: Excel worksheet COM object
        data_start_row: First data row (inclusive)
        data_end_row: Last data row (inclusive)
        formula_cols: List of column numbers that contain CIQ formulas
                      (accepts a single int for backwards compatibility)
        timeout: Max seconds to wait (default 600 = 10 min)
        check_interval: Seconds between checks
    """
    # Accept a single column for backwards compatibility
    if isinstance(formula_cols, int):
        formula_cols = [formula_cols]

    total_rows = data_end_row - data_start_row + 1
    total_cells = total_rows * len(formula_cols)
    print(f"Waiting for CIQ formulas to calculate...")
    print(f"  Monitoring {total_rows} rows × {len(formula_cols)} formula columns = {total_cells} cells")
    print(f"  Timeout: {timeout}s  (bulk read mode — {len(formula_cols)} COM calls per check)")

    start_time = time.time()

    while time.time() - start_time < timeout:
        pending = 0
        errors = 0
        success = 0
        empty = 0
        pending_locations = []

        # BULK READ: one COM call per formula column instead of per cell
        for col in formula_cols:
            col_vals = _read_column_bulk(ws, col, data_start_row, data_end_row)
            col_ltr = _col_letter(col)

            for row_offset, val in enumerate(col_vals):
                if _is_cell_pending(val):
                    pending += 1
                    if len(pending_locations) < 5:
                        pending_locations.append(f"{col_ltr}{data_start_row + row_offset}")
                elif val.startswith("#"):
                    errors += 1
                elif val.strip():
                    success += 1
                else:
                    empty += 1

        elapsed = int(time.time() - start_time)
        done = success + errors
        pct = round(done / total_cells * 100) if total_cells > 0 else 0

        status_line = (f"  {elapsed:3d}s: ✓ Done={done} ({pct}%), "
                       f"⏳ Pending={pending}, ✗ Errors={errors}, ○ Empty={empty}")
        if pending_locations:
            status_line += f"  [e.g. {', '.join(pending_locations)}]"
        print(status_line)

        if pending == 0 and (success > 0 or errors > 0):
            print(f"✓ All formulas finished calculating! ({done}/{total_cells} cells resolved)")
            return True

        # Trigger recalculation to nudge any stalled formulas
        try:
            ws.Calculate()
        except Exception:
            pass

        time.sleep(check_interval)

    # Timeout reached — report what's still pending
    print(f"⚠ Timeout after {timeout}s — {pending} cells still pending")
    if pending_locations:
        print(f"  Still pending at: {', '.join(pending_locations)}")
    return False


def run_capiq_enrichment(isins: List[str], template_path: str, output_path: str,
                         interactive: bool = True, batch_size: int = 2000) -> Optional[pd.DataFrame]:
    """
    Run Capital IQ enrichment using Excel template.

    Processes ISINs in batches to avoid overwhelming the CIQ add-in.
    Uses bulk COM operations for ~100x speedup over cell-by-cell access.

    Args:
        isins: List of ISINs to look up
        template_path: Path to Excel template with CIQ formulas
        output_path: Path to save CIQ results
        interactive: If True, prompt user in terminal. If False (server mode), proceed immediately.
        batch_size: Number of ISINs to process per batch (default 2000).
                    Smaller batches = less CIQ pressure, larger = fewer rounds.

    Returns:
        DataFrame with CIQ results, or None if failed
    """

    if not HAS_WIN32:
        raise RuntimeError("pywin32 not installed - cannot run Capital IQ enrichment")

    print("\n" + "=" * 60)
    print("STEP 2: Capital IQ Enrichment")
    print("=" * 60)

    # Prompt user to open Excel (only in CLI mode)
    if interactive:
        print("""
╔══════════════════════════════════════════════════════════════╗
║  MANUAL STEP REQUIRED                                        ║
║                                                              ║
║  Please do the following:                                    ║
║                                                              ║
║  1. Open Excel manually                                      ║
║  2. Make sure the Capital IQ add-in loads (check the ribbon) ║
║  3. Log into Capital IQ if prompted                          ║
║  4. Open your template file:                                 ║
║     {template}
║  5. Verify a test CIQ formula works                          ║
║  6. Keep Excel and the template OPEN                         ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
""".format(template=template_path))

        user_input = input("\nPress ENTER when Excel is ready (or type 'skip' to skip CIQ step): ").strip().lower()

        if user_input == 'skip':
            print("Skipping Capital IQ enrichment...")
            return None
    else:
        print("Server mode: proceeding with CIQ enrichment (user confirmed via dashboard)...")

    # Initialize COM
    pythoncom.CoInitialize()

    excel = None
    wb = None

    try:
        # Connect to existing Excel
        excel, _ = get_existing_excel()

        if excel is None:
            print("ERROR: Could not connect to Excel. Make sure Excel is open.")
            raise RuntimeError("Could not connect to Excel. Make sure Excel is running and the template is open.")

        # Find the template workbook
        template_name = os.path.basename(template_path)
        wb = None

        for workbook in excel.Workbooks:
            if template_name.lower() in workbook.Name.lower():
                wb = workbook
                print(f"✓ Found open workbook: {workbook.Name}")
                break

        if wb is None:
            open_names = [workbook.Name for workbook in excel.Workbooks]
            print(f"ERROR: Could not find '{template_name}' in open workbooks.")
            print("Open workbooks:", open_names)
            raise RuntimeError(
                f"Could not find '{template_name}' in open Excel workbooks. Open workbooks: {open_names}")

        ws = wb.ActiveSheet

        # Detect template structure
        header_row = 1
        template_row = 2
        isin_col = 1  # Column A

        # Find formula columns
        formula_cols = []
        for col_num in range(2, 27):
            cell_formula = ws.Cells(template_row, col_num).Formula
            if cell_formula and str(cell_formula).startswith('='):
                formula_cols.append(col_num)
                col_letter = chr(ord('A') + col_num - 1)
                print(f"  Found formula in column {col_letter}: {cell_formula[:50]}...")

        if not formula_cols:
            print("WARNING: No formulas found in template row 2!")
            raise RuntimeError(
                "No CIQ formulas found in row 2 of the template. Make sure the template has formulas in row 2.")

        last_col = max(formula_cols)

        # Read headers once (reused across batches)
        header_range = ws.Range(ws.Cells(header_row, 1), ws.Cells(header_row, last_col))
        header_vals = header_range.Value
        if isinstance(header_vals, tuple):
            headers = [str(v) if v else f"Column_{i+1}" for i, v in enumerate(header_vals[0])]
        else:
            headers = [str(header_vals) if header_vals else "Column_1"]

        # ── BATCHED PROCESSING ──
        # Split ISINs into batches to avoid overwhelming the CIQ add-in.
        # Each batch: clear → populate → calculate → wait → read → next batch
        isin_batches = list(chunk_list(isins, batch_size))
        total_batches = len(isin_batches)
        all_batch_data = []

        print(f"\n{'='*50}")
        print(f"Processing {len(isins)} ISINs in {total_batches} batch(es) of up to {batch_size}")
        print(f"{'='*50}")

        overall_start = time.time()

        for batch_idx, batch_isins in enumerate(isin_batches):
            batch_num = batch_idx + 1
            batch_len = len(batch_isins)
            print(f"\n── Batch {batch_num}/{total_batches} ({batch_len} ISINs) ──")

            # Clear existing data below header row
            used_rows = ws.UsedRange.Rows.Count
            if used_rows > header_row:
                ws.Range(
                    ws.Cells(header_row + 1, 1),
                    ws.Cells(max(used_rows, header_row + 1), last_col)
                ).Clear()

            # Populate template formulas + ISINs with screen updating off
            excel.ScreenUpdating = False
            excel.Calculation = -4135  # xlCalculationManual

            try:
                # Copy template row down for this batch
                if batch_len > 1:
                    source_range = ws.Range(ws.Cells(template_row, 1), ws.Cells(template_row, last_col))
                    dest_range = ws.Range(ws.Cells(template_row, 1),
                                          ws.Cells(template_row + batch_len - 1, last_col))
                    source_range.Copy()
                    dest_range.PasteSpecial(Paste=-4123)
                    excel.CutCopyMode = False

                # BULK WRITE: all ISINs at once (single COM call)
                isin_array = tuple((isin,) for isin in batch_isins)
                isin_range = ws.Range(
                    ws.Cells(template_row, isin_col),
                    ws.Cells(template_row + batch_len - 1, isin_col)
                )
                isin_range.Value = isin_array
                print(f"  ✓ Populated {batch_len} ISINs (bulk write)")
            finally:
                excel.ScreenUpdating = True
                excel.Calculation = -4105  # xlCalculationAutomatic

            # Trigger calculation
            print("  Triggering calculation...")
            excel.CalculateFull()

            # Wait for formulas — pass ALL formula columns so every cell is monitored
            data_end_row = template_row + batch_len - 1
            formulas_done = wait_for_ciq_formulas(
                ws, template_row, data_end_row, formula_cols, timeout=600
            )

            # Final calculation pass + settle
            print("  Final calculation pass...")
            excel.CalculateFull()
            time.sleep(5)

            if not formulas_done:
                print("  Running additional wait after final calculation...")
                formulas_done = wait_for_ciq_formulas(
                    ws, template_row, data_end_row, formula_cols, timeout=120
                )

            # BULK READ: read all batch data in single COM call
            print("  Reading results (bulk read)...")
            data_range = ws.Range(ws.Cells(template_row, 1), ws.Cells(data_end_row, last_col))
            raw_data = data_range.Value

            if raw_data is None:
                batch_data = []
            elif not isinstance(raw_data, tuple):
                batch_data = [{headers[0]: raw_data}]
            else:
                batch_data = []
                for row_tuple in raw_data:
                    row_data = {}
                    for col_idx, val in enumerate(row_tuple):
                        if col_idx < len(headers):
                            row_data[headers[col_idx]] = val
                    batch_data.append(row_data)

            print(f"  ✓ Read {len(batch_data)} rows × {len(headers)} columns")
            all_batch_data.extend(batch_data)

            # Progress summary
            done_so_far = len(all_batch_data)
            elapsed = time.time() - overall_start
            if batch_idx > 0:
                eta = (elapsed / done_so_far) * (len(isins) - done_so_far)
                print(f"  Progress: {done_so_far}/{len(isins)} "
                      f"({done_so_far/len(isins)*100:.0f}%), "
                      f"ETA: {int(eta//60)}m {int(eta%60)}s")

        # ── All batches complete — assemble final DataFrame ──
        overall_elapsed = time.time() - overall_start
        print(f"\n✓ All {total_batches} batch(es) complete in "
              f"{int(overall_elapsed//60)}m {int(overall_elapsed%60)}s")

        ciq_df = pd.DataFrame(all_batch_data)

        # ── Post-read validation: detect any cells still showing pending ──
        pending_cells = 0
        pending_examples = []
        for idx, row_data in enumerate(all_batch_data):
            for col_name, val in row_data.items():
                if val and _is_cell_pending(str(val)):
                    pending_cells += 1
                    if len(pending_examples) < 10:
                        isin_val = row_data.get(headers[0], f"row {idx}")
                        pending_examples.append(f"{col_name} @ ISIN {isin_val}")

        if pending_cells > 0:
            print(f"\n⚠ WARNING: {pending_cells} cells still have pending/loading values!")
            print(f"  These cells were read before CIQ finished calculating.")
            for ex in pending_examples:
                print(f"    • {ex}")
            print(f"  Consider re-running with a longer timeout or smaller --ciq-batch-size.")

            # Replace pending values with None
            for col in ciq_df.columns:
                mask = ciq_df[col].apply(
                    lambda v: _is_cell_pending(str(v)) if pd.notna(v) else False
                )
                if mask.any():
                    ciq_df.loc[mask, col] = None
            print(f"  → Replaced {pending_cells} pending cells with blank values in output.")
        else:
            print(f"✓ Post-read validation passed — no pending cells detected.")

        # Save CIQ results
        ciq_df.to_excel(output_path, index=False)
        print(f"✓ CIQ results saved to: {output_path}")

        return ciq_df

    except Exception as e:
        print(f"ERROR in CIQ enrichment: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        pythoncom.CoUninitialize()


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='ISIN Bond Data Enrichment Pipeline')
    parser.add_argument('input_file', help='Input Excel file with ISINs')
    parser.add_argument('output_file', help='Output file for final enriched results')
    parser.add_argument('--template', help='Capital IQ Excel template file')
    parser.add_argument('--api-key', help='OpenFIGI API key (optional)')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of ISINs to process')
    parser.add_argument('--sheet-name', default='Bank AG', help='Sheet name to read ISINs from')
    parser.add_argument('--skip-openfigi', action='store_true', help='Skip OpenFIGI lookup')
    parser.add_argument('--skip-capiq', action='store_true', help='Skip Capital IQ enrichment')
    parser.add_argument('--ciq-batch-size', type=int, default=2000,
                        help='ISINs per CIQ batch (default 2000, reduce if CIQ stalls)')

    args = parser.parse_args()

    print("""
╔══════════════════════════════════════════════════════════════╗
║           ISIN Bond Data Enrichment Pipeline                 ║
╚══════════════════════════════════════════════════════════════╝
""")

    # -------------------------------------------------------------------------
    # Load ISINs from input file
    # -------------------------------------------------------------------------
    print("Loading ISINs from input file...")

    try:
        df = pd.read_excel(args.input_file, sheet_name=args.sheet_name,
                           nrows=args.limit, engine='openpyxl')
        print(f"  File: {args.input_file}")
        print(f"  Sheet: {args.sheet_name}")
        print(f"  Rows: {len(df)}")
    except Exception as e:
        print(f"ERROR loading input file: {e}")
        return 1

    # Find ISIN column
    isin_col = None
    for col in df.columns:
        if 'ISIN' in col.upper():
            isin_col = col
            break

    if isin_col is None:
        isin_col = df.columns[0]
        print(f"  No 'ISIN' column found, using: '{isin_col}'")

    isins = df[isin_col].dropna().astype(str).str.strip().tolist()
    print(f"  Found {len(isins)} ISINs")

    if not isins:
        print("ERROR: No ISINs found!")
        return 1

    # -------------------------------------------------------------------------
    # Step 1: OpenFIGI Lookup
    # -------------------------------------------------------------------------
    openfigi_results = None

    if not args.skip_openfigi:
        openfigi_results = run_openfigi_lookup(isins, args.api_key)
        openfigi_df = pd.DataFrame(openfigi_results)

        # Save intermediate results
        openfigi_output = args.output_file.replace('.xlsx', '_openfigi.csv').replace('.csv', '_openfigi.csv')
        openfigi_df.to_csv(openfigi_output, index=False)
        print(f"OpenFIGI results saved to: {openfigi_output}")
    else:
        print("\nSkipping OpenFIGI lookup...")
        openfigi_df = pd.DataFrame({'ISIN': isins})

    # -------------------------------------------------------------------------
    # Step 2: Capital IQ Enrichment
    # -------------------------------------------------------------------------
    ciq_df = None

    if not args.skip_capiq and args.template and HAS_WIN32:
        ciq_output = args.output_file.replace('.xlsx', '_capiq.xlsx').replace('.csv', '_capiq.xlsx')
        ciq_df = run_capiq_enrichment(isins, args.template, ciq_output,
                                       batch_size=args.ciq_batch_size)
    elif not args.template:
        print("\nNo CIQ template specified - skipping Capital IQ enrichment")
        print("  Use --template to specify a CIQ template file")
    elif not HAS_WIN32:
        print("\nSkipping Capital IQ (pywin32 not available)")
    else:
        print("\nSkipping Capital IQ enrichment...")

    # -------------------------------------------------------------------------
    # Step 3: Merge Results
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 3: Merge Results")
    print("=" * 60)

    final_df = openfigi_df.copy()

    if ciq_df is not None and len(ciq_df) > 0:
        # Identify ISIN column in CIQ results
        ciq_isin_col = None
        for col in ciq_df.columns:
            if 'ISIN' in col.upper():
                ciq_isin_col = col
                break

        if ciq_isin_col is None:
            ciq_isin_col = ciq_df.columns[0]

        # Rename CIQ columns to avoid conflicts (except ISIN)
        ciq_rename = {}
        for col in ciq_df.columns:
            if col != ciq_isin_col and col in final_df.columns:
                ciq_rename[col] = f"CIQ_{col}"

        ciq_df = ciq_df.rename(columns=ciq_rename)

        # Merge on ISIN
        final_df = final_df.merge(
            ciq_df,
            left_on='ISIN',
            right_on=ciq_isin_col,
            how='left'
        )

        # Drop duplicate ISIN column if created
        if ciq_isin_col != 'ISIN' and ciq_isin_col in final_df.columns:
            final_df = final_df.drop(columns=[ciq_isin_col])

        print(f"✓ Merged CIQ data ({len(ciq_df.columns)} columns)")

    # -------------------------------------------------------------------------
    # Step 4: Apply Classifications (AFTER CIQ merge)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 4: Apply Classifications")
    print("=" * 60)

    final_df = apply_classifications(final_df)

    # -------------------------------------------------------------------------
    # Save Final Results
    # -------------------------------------------------------------------------
    if args.output_file.endswith('.xlsx'):
        final_df.to_excel(args.output_file, index=False)
    else:
        final_df.to_csv(args.output_file, index=False)

    print(f"\n✓ Final results saved to: {args.output_file}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total ISINs processed: {len(isins)}")
    print(f"Output columns: {len(final_df.columns)}")
    print(f"Output file: {args.output_file}")

    # Summary stats (now using final_df instead of openfigi_results)
    if 'Asset_Class' in final_df.columns:
        rate_count = sum(final_df['Asset_Class'] == 'Rate')
        credit_count = sum(final_df['Asset_Class'] == 'Credit')
        g10_count = sum(final_df['Market'] == 'G10')
        em_count = sum(final_df['Market'] == 'EM')
        print(f"\nFinal Classifications:")
        print(f"  Asset Class: Rate={rate_count}, Credit={credit_count}")
        print(f"  Market: G10={g10_count}, EM={em_count}")

    print("=" * 60)

    return 0


if __name__ == "__main__":
    ### 551f579d-8617-4dcd-99a6-0b0eef613734
    sys.exit(main())