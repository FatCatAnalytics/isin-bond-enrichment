"""
ISIN Bond Data Enrichment Pipeline
===================================
Combines OpenFIGI API lookup with Capital IQ Excel add-in enrichment.

Workflow:
    1. Run OpenFIGI lookup for basic bond data + classifications
    2. Prompt user to open Excel with CIQ template (add-in needs to load)
    3. Populate ISINs into CIQ template and wait for formulas
    4. Merge CIQ results with OpenFIGI data
    5. Output final enriched dataset

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

# G10 countries (by ISIN country prefix)
G10_COUNTRIES = {
    'US', 'GB', 'DE', 'FR', 'IT', 'JP', 'CA', 'BE', 'NL', 'SE', 'CH',
    'AU', 'AT', 'ES', 'IE', 'FI', 'PT', 'DK', 'NO', 'NZ', 'LU', 'XS',
}

# Market sectors that indicate government/sovereign bonds
GOVT_SECTORS = {'Govt', 'Muni'}

# Security types that typically indicate government bonds
GOVT_SECURITY_TYPES = {'DOMESTIC', 'Govt', 'T-Bill', 'Treasury', 'Sovereign'}


# ============================================================================
# CLASSIFICATION FUNCTIONS
# ============================================================================

def get_country_from_isin(isin: str) -> str:
    """Extract 2-letter country code from ISIN."""
    if isin and len(isin) >= 2:
        return isin[:2].upper()
    return ''


def classify_market(isin: str) -> str:
    """Classify as G10 or EM based on ISIN country code."""
    country = get_country_from_isin(isin)
    return 'G10' if country in G10_COUNTRIES else 'EM'


def classify_asset_class(market_sector: str, security_type: str, security_type2: str) -> str:
    """
    Classify as Rate or Credit based on market sector and security type.
    Rate = Government bonds (sovereign, treasury, muni)
    Credit = Corporate bonds, structured products, etc.
    """
    if market_sector in GOVT_SECTORS:
        return 'Rate'

    for sec_type in [security_type, security_type2]:
        if sec_type:
            sec_type_upper = sec_type.upper()
            for govt_indicator in GOVT_SECURITY_TYPES:
                if govt_indicator.upper() in sec_type_upper:
                    return 'Rate'

    return 'Credit'


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
        'Asset_Class': '',
        'Market': classify_market(isin),
    }

    if 'data' in result and result['data']:
        match = result['data'][0]
        market_sector = match.get('marketSector', '')
        security_type = match.get('securityType', '')
        security_type2 = match.get('securityType2', '')
        return {
            'ISIN': isin,
            'Name': match.get('name', ''),
            'Ticker': match.get('ticker', ''),
            'Security_Type': security_type,
            'Security_Type2': security_type2,
            'Market_Sector': market_sector,
            'Exchange_Code': match.get('exchCode', ''),
            'FIGI': match.get('figi', ''),
            'Composite_FIGI': match.get('compositeFIGI', '') or '',
            'Security_Description': match.get('securityDescription', ''),
            'Num_Matches': len(result['data']),
            'Asset_Class': classify_asset_class(market_sector, security_type, security_type2),
            'Market': classify_market(isin),
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
                    'Asset_Class': '', 'Market': classify_market(isin),
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


def wait_for_ciq_formulas(ws, data_start_row, data_end_row, formula_col, timeout=300, check_interval=10):
    """Wait for CIQ formulas to calculate."""
    print(f"Waiting for CIQ formulas to calculate (timeout: {timeout}s)...")

    start_time = time.time()

    while time.time() - start_time < timeout:
        requesting = 0
        errors = 0
        success = 0

        # Check first 50 rows
        for row in range(data_start_row, min(data_end_row + 1, data_start_row + 50)):
            try:
                val = str(ws.Cells(row, formula_col).Text or "")

                if "#REQUESTING" in val.upper() or "#GETTING" in val.upper():
                    requesting += 1
                elif val.startswith("#"):
                    errors += 1
                elif val:
                    success += 1
            except:
                pass

        elapsed = int(time.time() - start_time)
        print(f"  {elapsed:3d}s: ✓ Success={success}, ⏳ Loading={requesting}, ✗ Errors={errors}")

        if requesting == 0 and (success > 0 or errors > 0):
            print("✓ Formulas finished calculating!")
            return True

        try:
            ws.Calculate()
        except:
            pass

        time.sleep(check_interval)

    print(f"⚠ Timeout after {timeout}s")
    return False


def run_capiq_enrichment(isins: List[str], template_path: str, output_path: str, interactive: bool = True) -> Optional[pd.DataFrame]:
    """
    Run Capital IQ enrichment using Excel template.

    Args:
        isins: List of ISINs to look up
        template_path: Path to Excel template with CIQ formulas
        output_path: Path to save CIQ results
        interactive: If True, prompt user in terminal. If False (server mode), proceed immediately.

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
            raise RuntimeError(f"Could not find '{template_name}' in open Excel workbooks. Open workbooks: {open_names}")

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
            raise RuntimeError("No CIQ formulas found in row 2 of the template. Make sure the template has formulas in row 2.")

        last_col = max(formula_cols)

        # Clear existing data below template row
        used_rows = ws.UsedRange.Rows.Count
        if used_rows > template_row:
            print(f"Clearing rows {template_row + 1} to {used_rows}...")
            ws.Range(
                ws.Cells(template_row + 1, 1),
                ws.Cells(used_rows, last_col)
            ).Clear()

        # Copy template row down for all ISINs
        print(f"\nPopulating {len(isins)} ISINs...")

        if len(isins) > 1:
            source_range = ws.Range(ws.Cells(template_row, 1), ws.Cells(template_row, last_col))
            dest_range = ws.Range(ws.Cells(template_row, 1), ws.Cells(template_row + len(isins) - 1, last_col))
            source_range.Copy()
            dest_range.PasteSpecial(Paste=-4123)
            excel.CutCopyMode = False

        # Fill in ISINs
        for i, isin in enumerate(isins):
            ws.Cells(template_row + i, isin_col).Value = isin
            if (i + 1) % 500 == 0:
                print(f"  Populated {i + 1}/{len(isins)} ISINs...")

        print(f"✓ Populated all {len(isins)} ISINs")

        # Trigger calculation
        print("\nTriggering calculation...")
        excel.CalculateFull()

        # Wait for formulas
        data_end_row = template_row + len(isins) - 1
        wait_for_ciq_formulas(ws, template_row, data_end_row, formula_cols[0], timeout=300)

        # Final calculation
        print("Final calculation pass...")
        excel.CalculateFull()
        time.sleep(5)

        # Read results back into DataFrame
        print("Reading results...")

        # Get headers
        headers = []
        for col in range(1, last_col + 1):
            header = ws.Cells(header_row, col).Value
            headers.append(str(header) if header else f"Column_{col}")

        # Get data
        data = []
        for row in range(template_row, data_end_row + 1):
            row_data = {}
            for col_idx, col in enumerate(range(1, last_col + 1)):
                val = ws.Cells(row, col).Value
                row_data[headers[col_idx]] = val
            data.append(row_data)

        ciq_df = pd.DataFrame(data)

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
        ciq_df = run_capiq_enrichment(isins, args.template, ciq_output)
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

    if openfigi_results:
        rate_count = sum(1 for r in openfigi_results if r.get('Asset_Class') == 'Rate')
        credit_count = sum(1 for r in openfigi_results if r.get('Asset_Class') == 'Credit')
        g10_count = sum(1 for r in openfigi_results if r.get('Market') == 'G10')
        em_count = sum(1 for r in openfigi_results if r.get('Market') == 'EM')
        print(f"\nClassifications:")
        print(f"  Asset Class: Rate={rate_count}, Credit={credit_count}")
        print(f"  Market: G10={g10_count}, EM={em_count}")

    print("=" * 60)

    return 0


if __name__ == "__main__":

    ### 551f579d-8617-4dcd-99a6-0b0eef613734
    sys.exit(main())