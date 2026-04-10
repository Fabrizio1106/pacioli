"""
===============================================================================
Project: PACIOLI
Module: utils.parsers
===============================================================================

Description:
    Defensive parsers for the heterogeneous date and monetary formats that
    arrive in the bronze layer. Every function returns a safe default on
    failure (None or 0.0) so that the ETL is never aborted by a single
    malformed row.

Responsibilities:
    - Parse arbitrary date strings, including glued digits, separated
      formats and Excel serial numbers, into Python date objects.
    - Parse monetary values from Latin, US, SAP and dirty string formats
      into standard Python floats.
    - Parse bank datetime values covering common 12h/24h combinations.

Key Components:
    - parse_to_sql_date: Robust universal date parser for SQL loading.
    - parse_currency: Locale-aware currency parser with SAP trailing sign
      support.
    - parse_bank_datetime: Multi-format datetime parser for bank files.

Notes:
    - Dirty '.0' suffixes coming from Excel/CSV are stripped before parsing.
    - Currency parser disambiguates Latin ('1.200,50') and US ('1,200.50')
      formats by comparing the last comma and last dot positions.
    - Excel serial date support is clamped to the 1982-2064 range.

Dependencies:
    - pandas
    - datetime

===============================================================================
"""

import pandas as pd
from datetime import datetime, timedelta

def parse_to_sql_date(value):
    """
    Robust universal date parser safe for SQL loading.

    Args:
        value: Any scalar coming from Excel, CSV or text input.

    Returns:
        datetime.date or None: Parsed date, or None if the value is null,
        empty or does not match any supported pattern.

    Notes:
        Supported formats, in order of priority:
            1. Pandas Timestamp / datetime passthrough.
            2. Glued-digit formats DDMMYYYY and YYYYMMDD (8 digits).
            3. Missing-leading-zero case (7 digits treated as DMMYYYY).
            4. Separated formats (/, -, .): d/m/Y, Y-m-d, m/d/Y, Ymd, etc.
            5. Excel serial numbers in the 30000..60000 range (1982-2064).
        Dirty '.0' suffixes from Excel/CSV are stripped before parsing.
    """
    # 1. Null Validation
    if pd.isna(value):
        return None

    str_val = str(value).strip()
    
    if str_val.lower() in ("", "na", "n/a", "null", "none", "nan"):
        return None

    # 2. CRITICAL CLEANING: Remove the .0 decimal point if it comes from a dirty Excel/CSV file.
    # This transforms '20150331.0' into '20150331'
    if str_val.endswith('.0'):
        str_val = str_val[:-2]

    # 3. Pandas Timestamp
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.date()

    # 4. JOINED DIGIT STRATEGY (Without separators)
    # We clean up periods, slashes, and dashes
    clean_digits = str_val.replace('.', '').replace('/', '').replace('-', '')

    if clean_digits.isdigit():
        # CASE A: 8 Digits (Ex: 28112025 or 20150331)
        if len(clean_digits) == 8:
            # Attempt 1: Latin Format (DDMMYYYY) -> 28112025
            try:
                return datetime.strptime(clean_digits, "%d%m%Y").date()
            except ValueError:
                pass 
            
            # Attempt 2: ISO/Bank Format (YYYYMMDD) -> 20150331
            try:
                return datetime.strptime(clean_digits, "%Y%m%d").date()
            except ValueError:
                pass

        # CASE B: 7 Digits (Ex: 1122025 -> The leading zero is missing)
        if len(clean_digits) == 7:
            try:
                return datetime.strptime("0" + clean_digits, "%d%m%Y").date()
            except ValueError:
                pass

    # 5. SEPARATOR STRATEGY (With /, -, .)
    clean_val = str_val.split(" ")[0] # Remove time if it exists
    formats = [
        "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y",
        "%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(clean_val, fmt).date()
        except ValueError:
            continue

    # 6. SERIAL EXCEL (Pure Numbers)
    try:
        float_val = float(value)
        if 30000 <= float_val <= 60000: # Rango 1982-2064
            excel_epoch = datetime(1899, 12, 30)
            return (excel_epoch + timedelta(days=int(float_val))).date()
    except:
        pass

    return None


def parse_currency(value):
    """
    Convert any monetary representation (USD/EUR, Latin/US, SAP) to a
    standard Python float.

    Args:
        value: Scalar containing the monetary value in any supported
               textual or numeric representation.

    Returns:
        float: Parsed amount, or 0.0 on unrecognizable input.

    Notes:
        Automatically handled cases:
            - US      : "1,234.56"  -> 1234.56
            - Latin   : "1.234,56"  -> 1234.56
            - Simple  : "1,43"      -> 1.43
            - SAP neg : "100.00-"   -> -100.00
            - Dirty   : "USD 1,200" -> 1200.00
        Format disambiguation relies on comparing the last comma and
        last dot positions within the cleaned string. Currency tags
        (USD, EUR, $) are stripped before detection.
    """
    if pd.isna(value) or str(value).strip() == '':
        return 0.0

    # 1. Basic cleaning: Convert to string, uppercase, and remove coins/spaces
    str_val = str(value).upper().strip()
    str_val = str_val.replace('USD', '').replace('EUR', '').replace('$', '').strip()

    # 2. Handling negative sign at the end (SAP style: "500.00-")
    is_negative = False
    if str_val.endswith('-'):
        is_negative = True
        str_val = str_val[:-1] # We remove the minus sign from the end
    elif str_val.startswith('-'):
        is_negative = True
        str_val = str_val[1:]  # We remove the minus sign from the beginning

    # 3. FORMAT DETECTION
    # We look for the position of the last comma and the last period.
    last_comma_index = str_val.rfind(',')
    last_dot_index = str_val.rfind('.')

    try:
        if last_comma_index > last_dot_index:
            # LATIN/EUROPEAN CASE: The comma is AFTER the period (or there is no period)
            # Examples: "1,200.50" or "1.43"
            # Action: Delete periods (thousands) and change comma to period (decimal)
            clean_val = str_val.replace('.', '').replace(',', '.')
        else:
            # AMERICAN CASE: The period is AFTER the comma (or there is no comma)
            # Examples: "1,200.50" or "1000.50"
            # Action: Delete commas (thousands), the period is already decimal.
            clean_val = str_val.replace(',', '')

        # 4. Final conversion
        float_val = float(clean_val)
        
        # Apply negative sign if it existed
        return -float_val if is_negative else float_val

    except ValueError:
        # If it fails (e.g., unrecognizable text), we return 0.0 to avoid breaking the ETL.
        return 0.0

def parse_bank_datetime(value):
    """
    Parse a bank datetime value trying multiple ordered formats.

    Args:
        value: Scalar with the bank timestamp (text or datetime).

    Returns:
        datetime.datetime or None: Parsed datetime, or None when the value
        is null, empty or cannot be interpreted by any known format or by
        the pandas fallback.

    Notes:
        Formats are tried from most to least specific and cover both 12h
        and 24h representations in Latin and US ordering.
    """
    if pd.isna(value) or str(value).strip() == '' or str(value).lower() == 'nan':
        return None
    
    # If Excel already read it as a date, we return directly.
    if isinstance(value, (pd.Timestamp, datetime)):
        return value

    str_val = str(value).strip()

    # List of likely formats (High -> Low Priority)
    formats = [
        "%m/%d/%Y %I:%M:%S %p",  # 01/01/2026 05:38:00 AM
        "%d/%m/%Y %I:%M:%S %p",  # 31/01/2026 05:38:00 PM
        "%d/%m/%Y %H:%M:%S",     # 01/01/2021 14:25:00 (Format 24h)
        "%m/%d/%Y %H:%M:%S",     # 01/31/2021 14:25:00
        "%Y-%m-%d %H:%M:%S",     # 2021-01-01 14:25:00
        "%m/%d/%Y",              # 12/31/2025
        "%d/%m/%Y",              # 31/12/2025
        "%Y-%m-%d"               # 2025-12-31
    ]

    for fmt in formats:
        try:
            return datetime.strptime(str_val, fmt)
        except ValueError:
            continue
    
    # Pandas Smart Fallback
    try:
        return pd.to_datetime(str_val).to_pydatetime()
    except (ValueError, TypeError):
        return None