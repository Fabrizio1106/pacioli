"""
===============================================================================
Project: PACIOLI
Module: utils.excel_normalizer
===============================================================================

Description:
    Windows COM-based Excel normalizer used to repair legacy .xls files
    that pandas cannot parse directly. Delegates to Excel itself via
    pywin32 to open and resave the file as modern .xlsx (OpenXML) in the
    system temporary folder.

Responsibilities:
    - Wrap Excel COM lifecycle (Dispatch, open, save, quit).
    - Produce a unique temporary .xlsx path in the system TEMP directory.
    - Ensure Excel resources (workbook/application) are always released,
      even on failure.

Key Components:
    - normalize_to_temp_xlsx: Convert a legacy .xls file into a temporary
      .xlsx file and return its path.

Notes:
    - Requires Microsoft Excel installed on the host (Windows only).
    - pythoncom.CoInitialize/CoUninitialize bracket the COM calls.

Dependencies:
    - os, time, tempfile
    - pythoncom, win32com.client (pywin32)

===============================================================================
"""

import os
import time
import tempfile
import pythoncom
import win32com.client as win32

# Format .xlsx (OpenXML)
xlOpenXMLWorkbook = 51

def normalize_to_temp_xlsx(file_path, logger=None):
    """
    Open a legacy .xls file via Excel COM and save it as .xlsx in TEMP.

    Args:
        file_path (str): Path to the source .xls file.
        logger (callable, optional): Logger function accepting
            (message, level) for progress and error reporting.

    Returns:
        str: Absolute path to the generated temporary .xlsx file.

    Raises:
        Exception: Re-raises any exception thrown by Excel COM after
                   logging a CRITICAL message.

    Side Effects:
        - Launches a hidden Excel instance with DisplayAlerts disabled.
        - Creates a new file in the system TEMP directory.
        - Always releases Excel COM resources in the finally block.
    """
    # 1. Normalize the input path to avoid escape errors
    abs_path = os.path.abspath(file_path)
    filename = os.path.basename(abs_path)
    name_no_ext = os.path.splitext(filename)[0]
    
    # 2. Securely obtain the system's TEMP path
    # We use os.path.join to make Python handle slashes correctly
    temp_dir = tempfile.gettempdir()
    output_filename = f"{name_no_ext}_{int(time.time())}.xlsx"
    output_path = os.path.join(temp_dir, output_filename)
    
    # Clean up any traces of extraneous escape characters from the log
    if logger: logger(f"🔧 Reparando hacia Staging (Temp): {output_path.replace('\\', '/')}", "INFO")

    pythoncom.CoInitialize()
    excel = None
    workbook = None
    
    try:
        excel = win32.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False 
        
        # Open Original (.xls)
        workbook = excel.Workbooks.Open(abs_path)
        
        # Save as (.xlsx) in the TEMP folder
        workbook.SaveAs(output_path, FileFormat=xlOpenXMLWorkbook)
        
    except Exception as e:
        if logger: logger(f"❌ Error normalizando: {str(e)}", "CRITICAL")
        raise e
        
    finally:
        if workbook:
            try: workbook.Close(SaveChanges=False)
            except: pass
        if excel:
            try: excel.Quit()
            except: pass
        
        # Release resources
        del workbook
        del excel
        pythoncom.CoUninitialize()
            
    return output_path