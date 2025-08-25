import streamlit as st
import zipfile
import os
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import shutil
import io
import warnings
import time
from hrpt import process_files

# ---------------- Page Config ---------------- #
st.set_page_config(page_title="Hyundai Report Generator", layout="wide", initial_sidebar_state="expanded")

st.title("🚗 Hyundai Order Generator")
st.markdown("""
📊 Generate comprehensive reports from Hyundai data files including:
- OEM Reports
- Stock Reports
- Receiving Pending list
- Receiving Pending Detail
- Receiving Today List
- Receiving Today Detail
- Transfer List
- Transfer Detail
""")

# ---------------- Session State Init ---------------- #
state_vars = [
    "uploaded_file", "extracted_path", "validation_errors", "period_validation_errors",
    "missing_files", "validation_log", "continue_processing", "processing_complete",
    "report_results", "show_reports", "oem_mismatches","Receving_Pending_Detail_mismatches",
    "Transfer_List_mismatches","Receving_Today_Detail_mismatches", "Receving_Pending_list_mismatches",
    # new flags
    "suppress_validation_display", "input_signature",
    # NEW: blocking cross-sum validations
    "qty_mismatch_errors", "qty_mismatch_log"
]
for var in state_vars:
    if var not in st.session_state:
        if var in ["validation_errors", "period_validation_errors", "missing_files", "qty_mismatch_errors"]:
            st.session_state[var] = []
        elif var in ["validation_log", "oem_mismatches", "Receving_Pending_Detail_mismatches",
                     "Transfer_List_mismatches","Receving_Today_Detail_mismatches", "Receving_Pending_list_mismatches",
                     "qty_mismatch_log"]:
            st.session_state[var] = pd.DataFrame()
        elif var in ["continue_processing", "processing_complete", "show_reports",
                     "suppress_validation_display"]:
            st.session_state[var] = False
        elif var == "report_results":
            st.session_state[var] = None
        else:
            st.session_state[var] = None

# ---------------- Period Mapping ---------------- #
PERIOD_TYPES = {"Day": 1, "Week": 7, "Month": 30, "Quarter": 90, "Year": 365}

# ---------------- File Readers ---------------- #
def read_file(file_path, header=None):
    try:
        lower = file_path.lower()
        if lower.endswith('.xlsx'):
            return pd.read_excel(file_path, header=header, engine='openpyxl')
        if lower.endswith('.xls'):
            try:
                return pd.read_excel(file_path, header=header, engine='xlrd')
            except Exception:
                return pd.read_excel(file_path, header=header, engine='openpyxl')
        # CSV fallback
        try:
            return pd.read_csv(file_path, header=header, encoding='utf-8', sep=None, engine='python', on_bad_lines='skip')
        except UnicodeDecodeError:
            return pd.read_csv(file_path, header=header, encoding='windows-1252', sep=None, engine='python', on_bad_lines='skip')
    except Exception:
        return None

def try_read_as_csv(file_path, header=None):
    try:
        return pd.read_csv(file_path, header=header,encoding='utf-8', sep=None, engine='python', on_bad_lines='skip')
    except UnicodeDecodeError:
        try:
            return pd.read_csv(file_path, header=header,encoding='windows-1252', sep=None, engine='python', on_bad_lines='skip')
        except Exception as e:
            print(f"CSV read failed for {file_path}: {e}")
            return None

# ---------------- Validation Functions (periods) ---------------- #
def validate_periods(all_locations, start_date, end_date, period_days):
    validation_errors = []
    missing_periods_log = []

    # Build (start,end) windows
    periods = []
    current_date = start_date
    while current_date <= end_date:
        period_end = min(current_date + timedelta(days=period_days - 1), end_date)
        periods.append((current_date, period_end))
        current_date = period_end + timedelta(days=1)

    for brand, dealer, location, location_path in all_locations:
        # accept both "receiving" and common typo "receving"
        def startswith_either(name, prefix):
            return name.lower().startswith(prefix) or name.lower().startswith(prefix.replace("receiving","receving"))

        oem_files = [f for f in os.listdir(location_path) if f.lower().startswith('bo list')]
        rpd_files = [f for f in os.listdir(location_path) if startswith_either(f,'receiving pending detail')]
        rtd_files = [f for f in os.listdir(location_path) if startswith_either(f,'receiving today detail')]
        tl_files  = [f for f in os.listdir(location_path) if f.lower().startswith('transfer list')]

        # If any of the core files is completely absent, skip period checks for this location
        if not oem_files or not rpd_files or not rtd_files or not tl_files:
            continue

        # OEM (BO LIST) period coverage
        oem_has_period = {p: False for p in periods}
        for oem_file in oem_files:
            try:
                custom_headers = [
                    'ORDER NO', 'LINE', 'PART NO_ORDER', 'PART NO_CURRENT', 'PART NAME',
                    'PARTSOURCE', 'QUANTITY_ORDER', 'QUANTITY_CURRENT', 'B/O', 'PO DATE',
                    'PDC', 'ETA', 'MSG', 'PROCESSING_ALLOCATION', 'PROCESSING_ON-PICK',
                    'PROCESSING_ON-PACK', 'PROCESSING_PACKED', 'PROCESSING_INVOICE',
                    'PROCESSING_SHIPPEO', 'LOST QTY', 'ELAP'
                ]
                oem_df = read_file(os.path.join(location_path, oem_file), header=1)
                if oem_df is None or oem_df.empty:
                    continue
                oem_df.columns = custom_headers[:oem_df.shape[1]]
                if 'PO DATE' not in oem_df.columns:
                    continue
                oem_df['PO DATE'] = pd.to_datetime(oem_df['PO DATE'], errors='coerce')
                for p in periods:
                    period_start, period_end = p
                    if any(period_start <= d.date() <= period_end for d in oem_df['PO DATE'].dropna()):
                        oem_has_period[p] = True
            except Exception as e:
                validation_errors.append(f"{location}: Error validating OEM periods - {str(e)}")

        # Receiving Pending Detail coverage
        receiving_has_period = {p: False for p in periods}
        for rpd_file in rpd_files:
            try:
                cols = ['SEQ','CASE NO ','ORDER NO ','LINE NO','PART NO _SUPPLY','PART NO _ORDER','H/K','PART NAME',
                        'SUPPLY QTY','ORDER QTY','ACCEPT QTY','CLAIM QTY','CLAIM TYPE','CLAIM CODE','LOC','LIST PRICE',
                        'NDP (UNIT)','ED (UNIT)','MAT VALUE','DEPOT S/C','VOR S/C','OTHER CHARGES','STAX(%)','CTAX(%)',
                        'ITAX(%)','TAX(%)','HSN CODE','TAX AMT','FRT/INS','SGST AMT','CGST AMT','IGST AMT','COMP CESS AMT',
                        'LANDED COST','ORDER DATE','RECEIVING DATE','STATUS']
                rpd_df = read_file(os.path.join(location_path, rpd_file), header=1)
                if rpd_df is None or rpd_df.empty:
                    continue
                rpd_df.columns = cols[:rpd_df.shape[1]]
                if 'ORDER DATE' not in rpd_df.columns:
                    continue
                rpd_df['ORDER DATE'] = pd.to_datetime(rpd_df['ORDER DATE'], errors='coerce')
                for p in periods:
                    period_start, period_end = p
                    if any(period_start <= d.date() <= period_end for d in rpd_df['ORDER DATE'].dropna()):
                        receiving_has_period[p] = True
            except Exception as e:
                validation_errors.append(f"{location}: Error validating receiving periods - {str(e)}")

        # Receiving Today Detail coverage
        rtd_has_period = {p: False for p in periods}
        for rtd_file in rtd_files:
            try:
                cols = ['SEQ','CASE NO ','ORDER NO ','LINE NO','PART NO _SUPPLY','PART NO _ORDER','H/K','PART NAME',
                        'SUPPLY QTY','ORDER QTY','ACCEPT QTY','CLAIM QTY','CLAIM TYPE','CLAIM CODE','LOC','LIST PRICE',
                        'NDP (UNIT)','ED (UNIT)','MAT VALUE','DEPOT S/C','VOR S/C','OTHER CHARGES','STAX(%)','CTAX(%)',
                        'ITAX(%)','TAX(%)','HSN CODE','TAX AMT','FRT/INS','SGST AMT','CGST AMT','IGST AMT','COMP CESS AMT',
                        'LANDED COST','ORDER DATE','RECEIVING DATE','STATUS']
                rtd_df = read_file(os.path.join(location_path, rtd_file), header=1)
                if rtd_df is None or rtd_df.empty:
                    continue
                rtd_df.columns = cols[:rtd_df.shape[1]]
                if 'ORDER DATE' not in rtd_df.columns:
                    continue
                rtd_df['ORDER DATE'] = pd.to_datetime(rtd_df['ORDER DATE'], errors='coerce')
                for p in periods:
                    period_start, period_end = p
                    if any(period_start <= d.date() <= period_end for d in rtd_df['ORDER DATE'].dropna()):
                        rtd_has_period[p] = True
            except Exception as e:
                validation_errors.append(f"{location}: Error validating receiving today periods - {str(e)}")

        # Transfer List coverage
        tl_has_period = {p: False for p in periods}
        for tl in tl_files:
            try:
                cols = ['TRANSFER NO','REQ.DATE','REQ.TIME','SEND DATE','SEND.TIME','RECE.DATE','RECE.TIME','REQU.DEALER',
                        'SEND DEALER','ITEM_REQ','ITEM_SEND','QUANTITY_REQ','QUANTITY_SEND','AMOUNT','AMOUNT2',
                        'TAXABLE AMT','SGST AMT','CGST AMT','IGST AMT','COMP CESS AMT','STATUS']
                tl_df = read_file(os.path.join(location_path, tl), header=1)
                if tl_df is None or tl_df.empty:
                    continue
                tl_df.columns = cols[:tl_df.shape[1]]
                if 'REQ.DATE' not in tl_df.columns:
                    continue
                tl_df['REQ.DATE'] = pd.to_datetime(tl_df['REQ.DATE'], errors='coerce')
                for p in periods:
                    period_start, period_end = p
                    if any(period_start <= d.date() <= period_end for d in tl_df['REQ.DATE'].dropna()):
                        tl_has_period[p] = True
            except Exception as e:
                validation_errors.append(f"{location}: Error validating Transfer list periods - {str(e)}")

        # MRN not in Hyundai set; mark True
        mrn_has_period = {p: True for p in periods}

        for period_start, period_end in periods:
            missing_in = []
            if not oem_has_period[(period_start, period_end)]: missing_in.append("OEM")
            if not mrn_has_period[(period_start, period_end)]: missing_in.append("MRN")
            if not receiving_has_period[(period_start, period_end)]: missing_in.append("Receiving Pending Detail")
            if not rtd_has_period[(period_start, period_end)]: missing_in.append("Receiving Today Detail")
            if not tl_has_period[(period_start, period_end)]: missing_in.append("Transfer list")

            if missing_in:
                missing_periods_log.append({
                    'Brand': brand, 'Dealer': dealer, 'Location': location,
                    'Period': f"{period_start} to {period_end}",
                    'Missing In': ", ".join(missing_in)
                })
                validation_errors.append(f"{location}: {' and '.join(missing_in)} missing for period {period_start} to {period_end}")

    validation_log_df = pd.DataFrame(missing_periods_log) if missing_periods_log else pd.DataFrame(
        columns=['Brand', 'Dealer', 'Location', 'Period', 'Missing In']
    )
    return validation_errors, validation_log_df

# ---------------- HARD BLOCK: cross-sum checks ---------------- #
def _to_num(s):
    return pd.to_numeric(s, errors="coerce").fillna(0.0)

def validate_cross_sums(all_locations):
    """
    1) Sum(ACCEPT) in Receiving Pending List == Sum(ACCEPT QTY) in Receiving Pending Detail
    2) Sum(ACCEPT) in Receiving Today List   == Sum(ACCEPT QTY) in Receiving Today Detail
    3) Sum(SEND)   in Transfer List          == Sum(QUANTITY) in Transfer Detail
    If any mismatch -> return blocking errors.
    """
    errors = []
    rows = []

    # canonical headers used in your pipeline
    RPL_COLS = ['SEQ','H/K','GR_NO','GR_TYPE','GR_STATUS','INVOICE_NO','INVOICE_DATE',
                'SHIPPED INFORMATION_SUPPLIER','SHIPPED INFORMATION_TRUCK NO','SHIPPED INFORMATION_CARRIER NAME',
                'SHIPPED INFORMATION_FINISH DATE','SHIPPED INFORMATION_ACCEPT QTY','SHIPPED INFORMATION_CLAIM QTY',
                'SHIPPED INFORMATION_MAT VALUE','SHIPPED INFORMATION_FREIGHT AMT','SHIPPED INFORMATION_SGST AMT',
                'SHIPPED INFORMATION_IGST AMT','SHIPPED INFORMATION_TCS AMT','SHIPPED INFORMATION_TAX AMOUNT']

    RPD_COLS = ['SEQ','CASE NO ','ORDER NO ','LINE NO','PART NO _SUPPLY','PART NO _ORDER','H/K','PART NAME',
                'SUPPLY QTY','ORDER QTY','ACCEPT QTY','CLAIM QTY','CLAIM TYPE','CLAIM CODE','LOC','LIST PRICE',
                'NDP (UNIT)','ED (UNIT)','MAT VALUE','DEPOT S/C','VOR S/C','OTHER CHARGES','STAX(%)','CTAX(%)',
                'ITAX(%)','TAX(%)','HSN CODE','TAX AMT','FRT/INS','SGST AMT','CGST AMT','IGST AMT','COMP CESS AMT',
                'LANDED COST','ORDER DATE','RECEIVING DATE','STATUS']

    TL_COLS = ['TRANSFER NO','REQ.DATE','REQ.TIME','SEND DATE','SEND.TIME','RECE.DATE','RECE.TIME','REQU.DEALER',
               'SEND DEALER','ITEM_REQ','ITEM_SEND','QUANTITY_REQ','QUANTITY_SEND','AMOUNT','AMOUNT2','TAXABLE AMT',
               'SGST AMT','CGST AMT','IGST AMT','COMP CESS AMT','STATUS']

    for brand, dealer, location, location_path in all_locations:
        # helper to accept both spellings
        def pick_files(prefix):
            return [f for f in os.listdir(location_path)
                    if f.lower().startswith(prefix) or f.lower().startswith(prefix.replace("receiving","receving"))]

        # ----- 1) Receiving Pending List vs Detail -----
        rpl_files = pick_files('receiving pending list')
        rpd_files = pick_files('receiving pending detail')

        rpl_accept = 0.0
        for f in rpl_files:
            df = read_file(os.path.join(location_path, f), header=2)
            if df is None or df.empty: continue
            df.columns = RPL_COLS[:df.shape[1]]
            #df['SHIPPED INFORMATION_ACCEPT QTY']=df['SHIPPED INFORMATION_ACCEPT QTY'].astype(float).fillna(0.0)
            #st.dataframe(df)
            if 'SHIPPED INFORMATION_ACCEPT QTY' in df.columns:
                rpl_accept += _to_num(df['SHIPPED INFORMATION_ACCEPT QTY']).sum()
               # rpl_accept += df['SHIPPED INFORMATION_ACCEPT QTY'].astype(float).sum()

        rpd_accept = 0.0
        for f in rpd_files:
            df = read_file(os.path.join(location_path, f), header=1)
            if df is None or df.empty: continue
            df.columns = RPD_COLS[:df.shape[1]]
            if 'ACCEPT QTY' in df.columns:
                rpd_accept += _to_num(df['ACCEPT QTY']).sum()
                #rpd_accept += df['ACCEPT QTY'].astype(float).sum()

        if (rpl_files or rpd_files) and abs(rpl_accept - rpd_accept) > 1e-6:
            errors.append(f"{location}: Receiving Pending List ACCEPT({rpl_accept:.2f}) != Pending Detail ACCEPT QTY({rpd_accept:.2f})")
            rows.append({"Brand":brand,"Dealer":dealer,"Location":location,"Check":"Receiving Pending (List vs Detail)",
                        "List_Sum":rpl_accept,"Detail_Sum":rpd_accept,"Difference":rpl_accept - rpd_accept})

        # ----- 2) Receiving Today List vs Detail -----
        rtl_files = pick_files('receiving today list')
        rtd_files = pick_files('receiving today detail')

        rtl_accept = 0.0
        for f in rtl_files:
            df = read_file(os.path.join(location_path, f), header=2)
            if df is None or df.empty: continue
            df.columns = RPL_COLS[:df.shape[1]]
            if 'SHIPPED INFORMATION_ACCEPT QTY' in df.columns:
                #tl_accept += _to_num(df['SHIPPED INFORMATION_ACCEPT QTY']).sum()
                rtl_accept += _to_num(df['SHIPPED INFORMATION_ACCEPT QTY']).sum()
                #rtl_accept += df['SHIPPED INFORMATION_ACCEPT QTY'].astype(float).sum()

        rtd_accept = 0.0
        for f in rtd_files:
            df = read_file(os.path.join(location_path, f), header=1)
            if df is None or df.empty: continue
            df.columns = RPD_COLS[:df.shape[1]]
            if 'ACCEPT QTY' in df.columns:
                #rtd_accept += df['ACCEPT QTY'].astype(float).sum()
                rtd_accept += _to_num(df['ACCEPT QTY']).sum()

        if (rtl_files or rtd_files) and abs(rtl_accept - rtd_accept) > 1e-6:
            errors.append(f"{location}: Receiving Today List ACCEPT({rtl_accept:.2f}) != Today Detail ACCEPT QTY({rtd_accept:.2f})")
            rows.append({"Brand":brand,"Dealer":dealer,"Location":location,"Check":"Receiving Today (List vs Detail)",
                        "List_Sum":rtl_accept,"Detail_Sum":rtd_accept,"Difference":rtl_accept - rtd_accept})

        # ----- 3) Transfer List vs Transfer Detail -----
        tl_files = [f for f in os.listdir(location_path) if f.lower().startswith("transfer list")]
        td_files = [f for f in os.listdir(location_path) if f.lower().startswith("transfer detail")]

        tl_send = 0.0
        for f in tl_files:
            df = read_file(os.path.join(location_path, f), header=1)
            if df is None or df.empty: continue
            df.columns = TL_COLS[:df.shape[1]]
            if 'QUANTITY_SEND' in df.columns:
                #tl_send += df['QUANTITY_SEND'].astype(float).sum()
                tl_send += _to_num(df['QUANTITY_SEND']).sum()

        # Transfer Detail is less standardized; try common candidates
        td_qty = 0.0
        qty_candidates = ["QUANTITY", "QTY", "QUANTITY_SEND", "QUANTITY_REQ", "ITEM_SEND"]
        for f in td_files:
            df = read_file(os.path.join(location_path, f), header=0)
            if df is None or df.empty: continue
            # pick the first candidate present (case-sensitive as read)
            cand = next((c for c in qty_candidates if c in df.columns), None)
            if cand:
               # td_qty += df[cand].astype(float).sum()
                td_qty += _to_num(df[cand]).sum()

        if (tl_files or td_files) and abs(tl_send - td_qty) > 1e-6:
            errors.append(f"{location}: Transfer List SEND({tl_send:.2f}) != Transfer Detail QUANTITY({td_qty:.2f})")
            rows.append({"Brand":brand,"Dealer":dealer,"Location":location,"Check":"Transfer (List vs Detail)",
                        "List_Sum":tl_send,"Detail_Sum":td_qty,"Difference":tl_send - td_qty})

    log_df = pd.DataFrame(rows, columns=["Brand","Dealer","Location","Check","List_Sum","Detail_Sum","Difference"])
    return errors, log_df

# ---------------- Optional: external checks kept lenient ---------------- #
def validate_oem_mrn_po_codes(all_locations):
    """Safe/lenient for Hyundai; returns empty dataframes if structure not found."""
    try:
        df = pd.read_excel(
            r"https://docs.google.com/spreadsheets/d/e/2PACX-1vTeXEadE1Hf4G2T-o4XCvGYMyRKj6f2sVxsSDaPs_sJwmGbnCFoDzSJx9JHDaNzw5JKdk4l0Q0Yctmh/pub?output=xlsx"
        )
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    # Not used as a blocker here
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# ---------------- UI Functions ---------------- #
def show_validation_issues():
    # If suppressed, don't render (guard just in case)
    if st.session_state.get("suppress_validation_display", False):
        return

    # BLOCKING ERRORS (cross-sums)
    if st.session_state.qty_mismatch_errors:
        st.error("⛔ Quantity Reconciliation Errors (blocking)")
        for err in st.session_state.qty_mismatch_errors:
            st.write(f"- {err}")
        if not st.session_state.qty_mismatch_log.empty:
            st.download_button(
                "📥 Download Quantity Mismatch Log",
                data=st.session_state.qty_mismatch_log.to_csv(index=False).encode('utf-8'),
                file_name="quantity_mismatch_log.csv",
                mime="text/csv",
                key="dl_qty_log"
            )
        st.info("Please correct the files and upload again. Processing is halted.")
        return

    # file missing
    st.warning("⚠ Validation Issues Found")
    if st.session_state.missing_files:
        st.write("#### Missing Files:")
        for msg in st.session_state.missing_files:
            st.write(f"- {msg}")
    if st.session_state.period_validation_errors:
        st.write("#### Missing Period Data:")
        st.write(f"Found {len(st.session_state.period_validation_errors)} period validation issues")
        for error in st.session_state.period_validation_errors[:2]:
            st.write(f"- {error}")
        if len(st.session_state.period_validation_errors) > 2:
            st.write(f"- ... and {len(st.session_state.period_validation_errors)-2} more")

    col3, col4 = st.columns(2)
    with col3:
        if not st.session_state.validation_log.empty:
            st.download_button(
                "📥 Download Full Validation Log",
                data=st.session_state.validation_log.to_csv(index=False).encode('utf-8'),
                file_name="validation_issues_log.csv",
                mime="text/csv"
            )
    with col4:
        pass

    col1, col2 = st.columns(2)
    with col1:
        if st.button("✅ Continue Anyway", key="btn_continue_anyway"):
            st.session_state.continue_processing = True
            st.session_state.suppress_validation_display = True  
            st.rerun()
    with col2:
        if st.button("❌ Stop Processing"):
            st.session_state.continue_processing = False
            st.session_state.show_reports = False
            st.warning("Processing stopped by user")
            time.sleep(1)
            st.rerun()

def show_reports():
    st.success("🎉 Reports generated successfully!")
    if st.session_state.report_results:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_name, df in st.session_state.report_results.items():
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                zipf.writestr(file_name, excel_buffer.getvalue())
        st.download_button(
            "📦 Download All Reports as ZIP",
            data=zip_buffer.getvalue(),
            file_name="Hyundai_Reports.zip",
            mime="application/zip"
        )

# ---------------- Sidebar ---------------- #
with st.sidebar:
    st.header("⚙ Settings")
    uploaded_file = st.file_uploader("Upload Hyundai ZIP file", type=['zip'])
    if uploaded_file is not None:
        st.session_state.uploaded_file = uploaded_file

    select_categories = st.multiselect(
        "Choose categories",
        options=['Spares', 'Accessories', 'All'],
        default=['Spares']
    )

    default_end = datetime.today()
    default_start = default_end - timedelta(days=59)
    start_date = st.date_input("Start Date", value=default_start)
    end_date = st.date_input("End Date", value=default_end)
    period_type = st.selectbox("Select period type", options=list(PERIOD_TYPES.keys()))
    st.session_state.period_type = period_type
    process_btn = st.button("🚀 Generate Reports", type="primary")

# ---- Reset suppression flag when inputs change ----
sig_file = st.session_state.uploaded_file.name if st.session_state.uploaded_file else "nofile"
input_signature = f"{sig_file}|{start_date}|{end_date}|{st.session_state.period_type}|{tuple(sorted(select_categories))}"
if st.session_state.get("input_signature") != input_signature:
    st.session_state.input_signature = input_signature
    st.session_state.suppress_validation_display = False
    st.session_state.continue_processing = False

# ---------------- Main Processing ---------------- #
if (process_btn or st.session_state.continue_processing) and st.session_state.uploaded_file is not None:
    if st.session_state.uploaded_file.size > 200 * 1024 * 1024:
        st.error("File size exceeds 200MB limit")
        st.stop()

    temp_dir = tempfile.mkdtemp()
    extract_path = os.path.join(temp_dir, "extracted_files")
    os.makedirs(extract_path, exist_ok=True)

    try:
        with zipfile.ZipFile(st.session_state.uploaded_file, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        st.session_state.extracted_path = extract_path
        st.success("✅ ZIP file extracted successfully")

        all_locations = []
        for brand in os.listdir(extract_path):
            brand_path = os.path.join(extract_path, brand)
            if not os.path.isdir(brand_path): continue
            for dealer in os.listdir(brand_path):
                dealer_path = os.path.join(brand_path, dealer)
                if not os.path.isdir(dealer_path): continue
                for location in os.listdir(dealer_path):
                    location_path = os.path.join(dealer_path, location)
                    if os.path.isdir(location_path):
                        all_locations.append((brand, dealer, location, location_path))

        # file presence checks
        missing_files = []
        for brand, dealer, location, location_path in all_locations:
            required = {
                'bo list': False, 'receiving pending list': False, 'receiving pending detail': False, 'stock': False,
                'receiving today list': False, 'receiving today detail': False, 'transfer list': False, 'transfer detail': False
            }
            for file in os.listdir(location_path):
                f = file.lower()
                if f.startswith('bo list'): required['bo list'] = True
                if f.startswith('receiving today list') or f.startswith('receving today list'): required['receiving today list'] = True
                if f.startswith('receiving today detail') or f.startswith('receving today detail'): required['receiving today detail'] = True
                if f.startswith('transfer list'): required['transfer list'] = True
                if f.startswith('transfer detail'): required['transfer detail'] = True
                if f.startswith('receiving pending detail') or f.startswith('receving pending detail'): required['receiving pending detail'] = True
                if f.startswith('receiving pending list') or f.startswith('receving pending list'): required['receiving pending list'] = True
                if f.startswith('stock'): required['stock'] = True

            for k, v in required.items():
                if not v:
                    missing_files.append(f"{brand}/{dealer}/{location} - Missing: {k}")

        period_days = PERIOD_TYPES.get(st.session_state.period_type, 1)
        period_validation_errors, validation_log = validate_periods(all_locations, start_date, end_date, period_days)

        # HARD BLOCK: cross-sum validations
        qty_mismatch_errors, qty_mismatch_log = validate_cross_sums(all_locations)

        # save validation state
        st.session_state.missing_files = missing_files
        st.session_state.period_validation_errors = period_validation_errors
        st.session_state.validation_log = validation_log
        st.session_state.oem_mismatches = pd.DataFrame()
        st.session_state.Receving_Pending_Detail_mismatches = pd.DataFrame()
        st.session_state.Transfer_List_mismatches = pd.DataFrame()
        st.session_state.Receving_Today_Detail_mismatches = pd.DataFrame()
        st.session_state.Receving_Pending_list_mismatches = pd.DataFrame()
        st.session_state.qty_mismatch_errors = qty_mismatch_errors
        st.session_state.qty_mismatch_log = qty_mismatch_log

        # Process only if allowed (hard block ignores Continue Anyway)
        hard_block = bool(qty_mismatch_errors)
        can_process = (
            not hard_block and (
                st.session_state.continue_processing
                or (
                    not missing_files
                    and not period_validation_errors
                )
            )
        )

        if can_process:
            progress_bar = st.progress(0)
            status_text = st.empty()
            with st.spinner("Processing files..."):
                process_files([], all_locations, start_date, end_date, len(all_locations), progress_bar, status_text, select_categories)
                time.sleep(0.5)
            st.session_state.processing_complete = True
            st.session_state.show_reports = True
            st.session_state.continue_processing = False
        else:
            st.session_state.show_reports = False

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

# ---------------- Output ---------------- #
if st.session_state.uploaded_file is not None:
    # Show blocking/non-blocking validations as appropriate
    if (
        st.session_state.qty_mismatch_errors
        or st.session_state.missing_files
        or st.session_state.period_validation_errors
    ):
        show_validation_issues()





