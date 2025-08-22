def process_files(validation_errors, all_locations, start_date, end_date, total_locations,
                  progress_bar, status_text, select_categories):

    import streamlit as st
    import os
    import io
    import zipfile
    import pandas as pd
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Keep DataFrame previews separate from downloadable file bytes
    previews = {}  # name -> DataFrame
    files = {}     # name -> excel bytes

    # ---------- helpers ----------
    def read_file(file_path, header=None):
        try:
            lower = file_path.lower()
            if lower.endswith(".xlsx"):
                return pd.read_excel(file_path, header=header, engine="openpyxl")
            if lower.endswith(".xls"):
                try:
                    return pd.read_excel(file_path, header=header, engine="xlrd")
                except Exception:
                    return pd.read_excel(file_path, header=header, engine="openpyxl")
            # CSV / TXT best-effort
            try:
                return pd.read_csv(file_path, header=header, sep=None, engine="python",
                                   on_bad_lines="skip", encoding="utf-8")
            except UnicodeDecodeError:
                return pd.read_csv(file_path, header=header, sep=None, engine="python",
                                   on_bad_lines="skip", encoding="windows-1252")
        except Exception:
            return None

    def to_num(s):
        return pd.to_numeric(s, errors="coerce").fillna(0)

    # For Stock: handle common header variants
    STOCK_PART_COLS = ["PART NO ?", "PART NO", "PART NO.", "PART_NO", "PART NUMBER", "PART_NUMBER"]
    STOCK_QTY_COLS  = ["ON-HAND", "ON HAND", "ONHAND", "ON_HAND", "QTY", "CLOSE_QTY"]

    # ---------- per location ----------
    for i, (brand, dealer, location, location_path) in enumerate(all_locations):
        progress_bar.progress((i + 1) / max(total_locations, 1))
        status_text.text(f"Generating reports for {location} ({i+1}/{total_locations})...")

        BO_LIST = []
        Stock_data = []
        Receving_Pending_Detail = []
        Receving_Today_Detail = []
        Receving_Today_List = []
        Receving_Pending_list = []
        Transfer_List = []
        Transfer_Detail = []

        for file in os.listdir(location_path):
            file_path = os.path.join(location_path, file)
            if not os.path.isfile(file_path):
                continue

            fl = file.lower().strip()

            # BO LIST (header row is the 2nd row -> header=1)
            if fl.startswith("bo list"):
                custom_headers = [
                    'ORDER NO', 'LINE', 'PART NO_ORDER', 'PART NO_CURRENT', 'PART NAME',
                    'PARTSOURCE', 'QUANTITY_ORDER', 'QUANTITY_CURRENT', 'B/O', 'PO DATE',
                    'PDC', 'ETA', 'MSG', 'PROCESSING_ALLOCATION', 'PROCESSING_ON-PICK',
                    'PROCESSING_ON-PACK', 'PROCESSING_PACKED', 'PROCESSING_INVOICE',
                    'PROCESSING_SHIPPEO', 'LOST QTY', 'ELAP'
                ]
                bo_df = read_file(file_path, header=1)
                if bo_df is None or bo_df.empty:
                    validation_errors.append(f"{location}: Unable to read BO LIST -> {file}")
                    continue
                bo_df.columns = custom_headers[:bo_df.shape[1]]

                required_cols = ['ORDER NO', 'PART NO_CURRENT', 'PO DATE', 'QUANTITY_CURRENT', 'PROCESSING_ALLOCATION']
                missing = [c for c in required_cols if c not in bo_df.columns]
                if missing:
                    validation_errors.append(f"{location}: BO LIST missing columns - {', '.join(missing)}")
                    continue

                bo_df['__source_file__'] = file
                bo_df['Brand'] = brand
                bo_df['Dealer'] = dealer
                bo_df['Location'] = location
                BO_LIST.append(bo_df)
                continue

            # STOCK
            if fl.startswith("stock"):
                sd = read_file(file_path, header=0)
                if sd is None or sd.empty:
                    validation_errors.append(f"{location}: Unable to read Stock -> {file}")
                    continue

                # choose part / qty columns
                part_col = next((c for c in STOCK_PART_COLS if c in sd.columns), None)
                qty_col  = next((c for c in STOCK_QTY_COLS if c in sd.columns), None)
                if not part_col or not qty_col:
                    validation_errors.append(f"{location}: Stock file missing part/qty columns -> {file}")
                    continue

                sd['Brand'] = brand
                sd['Dealer'] = dealer
                sd['Location'] = location
                sd['__source_file__'] = file

                # Map PART TYPE -> New partcat
                if 'PART TYPE' in sd.columns:
                    sd['PART TYPE'] = sd['PART TYPE'].astype(str).str.strip()
                    sd['New partcat'] = sd['PART TYPE'].str.upper().map({'X': 'Spares','Y': 'Spares', 'A': 'Accessories'})
                else:
                    sd['New partcat'] = None

                # category filter
                # if select_categories!='All':
                #     sel = set([str(x).strip().lower() for x in select_categories])
                #     sd = sd[sd['New partcat'].astype(str).str.lower().isin(sel)]

                if select_categories==['Spares']:
                  sd = sd[sd['New partcat'].isin(select_categories)]
                elif select_categories==['Accessories']:
                  sd = sd[sd['New partcat'].isin(select_categories)]
                elif select_categories==['Spares','Accessories']:
                  sd = sd[sd['New partcat'].isin(select_categories)]
                elif select_categories==['All']:
                  sd=sd.copy()
                  
                out = sd[['Brand', 'Dealer', 'Location', part_col, qty_col]].copy()
                out.rename(columns={part_col: 'Partnumber', qty_col: 'Qty'}, inplace=True)
                out['Partnumber']=out['Partnumber'].astype(str).str.strip()
                out['Qty'] = to_num(out['Qty']).astype(float)
               
                Stock_data.append(out)
                continue

            # RECEIVING PENDING DETAIL (header=1)
            if fl.startswith("receving pending detail"):
                cols = ['SEQ','CASE NO ','ORDER NO ','LINE NO','PART NO _SUPPLY','PART NO _ORDER','H/K','PART NAME',
                        'SUPPLY QTY','ORDER QTY','ACCEPT QTY','CLAIM QTY','CLAIM TYPE','CLAIM CODE','LOC','LIST PRICE',
                        'NDP (UNIT)','ED (UNIT)','MAT VALUE','DEPOT S/C','VOR S/C','OTHER CHARGES','STAX(%)','CTAX(%)',
                        'ITAX(%)','TAX(%)','HSN CODE','TAX AMT','FRT/INS','SGST AMT','CGST AMT','IGST AMT','COMP CESS AMT',
                        'LANDED COST','ORDER DATE','RECEIVING DATE','STATUS']
                df = read_file(file_path, header=1)
                if df is None or df.empty:
                    continue
                df.columns = cols[:df.shape[1]]
                df['__source_file__'] = file
                df['Brand'] = brand
                df['Dealer'] = dealer
                df['Location'] = location
                Receving_Pending_Detail.append(df)
                continue

            # RECEIVING PENDING LIST (header=2)
            if fl.startswith("receving pending list"):
                cols = ['SEQ','H/K','GR_NO','GR_TYPE','GR_STATUS','INVOICE_NO','INVOICE_DATE','SHIPPED INFORMATION_SUPPLIER',
                        'SHIPPED INFORMATION_TRUCK NO','SHIPPED INFORMATION_CARRIER NAME','SHIPPED INFORMATION_FINISH DATE',
                        'SHIPPED INFORMATION_ACCEPT QTY','SHIPPED INFORMATION_CLAIM QTY','SHIPPED INFORMATION_MAT VALUE',
                        'SHIPPED INFORMATION_FREIGHT AMT','SHIPPED INFORMATION_SGST AMT','SHIPPED INFORMATION_IGST AMT',
                        'SHIPPED INFORMATION_TCS AMT','SHIPPED INFORMATION_TAX AMOUNT']
                df = read_file(file_path, header=2)
                if df is not None and not df.empty:
                    df.columns = cols[:df.shape[1]]
                    df['__source_file__'] = file
                    df['Brand'] = brand
                    df['Dealer'] = dealer
                    df['Location'] = location
                    Receving_Pending_list.append(df)
                continue

            # RECEIVING TODAY LIST (header=2)
            if fl.startswith("receving today list"):
                cols = ['SEQ','H/K','GR_NO','GR_TYPE','GR_STATUS','INVOICE_NO','INVOICE_DATE','SHIPPED INFORMATION_SUPPLIER',
                        'SHIPPED INFORMATION_TRUCK NO','SHIPPED INFORMATION_CARRIER NAME','SHIPPED INFORMATION_FINISH DATE',
                        'SHIPPED INFORMATION_ACCEPT QTY','SHIPPED INFORMATION_CLAIM QTY','SHIPPED INFORMATION_MAT VALUE',
                        'SHIPPED INFORMATION_FREIGHT AMT','SHIPPED INFORMATION_SGST AMT','SHIPPED INFORMATION_IGST AMT',
                        'SHIPPED INFORMATION_TCS AMT','SHIPPED INFORMATION_TAX AMOUNT']
                df = read_file(file_path, header=2)
                if df is not None and not df.empty:
                    df.columns = cols[:df.shape[1]]
                    df['__source_file__'] = file
                    df['Brand'] = brand
                    df['Dealer'] = dealer
                    df['Location'] = location
                    Receving_Today_List.append(df)
                continue

            # RECEIVING TODAY DETAIL (header=1)
            if fl.startswith("receving today detail"):
                cols = ['SEQ','CASE NO ','ORDER NO ','LINE NO','PART NO _SUPPLY','PART NO _ORDER','H/K','PART NAME',
                        'SUPPLY QTY','ORDER QTY','ACCEPT QTY','CLAIM QTY','CLAIM TYPE','CLAIM CODE','LOC','LIST PRICE',
                        'NDP (UNIT)','ED (UNIT)','MAT VALUE','DEPOT S/C','VOR S/C','OTHER CHARGES','STAX(%)','CTAX(%)',
                        'ITAX(%)','TAX(%)','HSN CODE','TAX AMT','FRT/INS','SGST AMT','CGST AMT','IGST AMT','COMP CESS AMT',
                        'LANDED COST','ORDER DATE','RECEIVING DATE','STATUS']
                df = read_file(file_path, header=1)
                if df is not None and not df.empty:
                    df.columns = cols[:df.shape[1]]
                    df['__source_file__'] = file
                    df['Brand'] = brand
                    df['Dealer'] = dealer
                    df['Location'] = location
                    Receving_Today_Detail.append(df)
                continue

            # TRANSFER LIST (header=1)
            if fl.startswith("transfer list"):
                cols = ['TRANSFER NO','REQ.DATE','REQ.TIME','SEND DATE','SEND.TIME','RECE.DATE','RECE.TIME','REQU.DEALER',
                        'SEND DEALER','ITEM_REQ','ITEM_SEND','QUANTITY_REQ','QUANTITY_SEND','AMOUNT','AMOUNT2','TAXABLE AMT',
                        'SGST AMT','CGST AMT','IGST AMT','COMP CESS AMT','STATUS']
                df = read_file(file_path, header=1)
                if df is not None and not df.empty:
                    df.columns = cols[:df.shape[1]]
                    df['__source_file__'] = file
                    df['Brand'] = brand
                    df['Dealer'] = dealer
                    df['Location'] = location
                    Transfer_List.append(df)
                continue

            # TRANSFER DETAIL (header=0)
            if fl.startswith("transfer detail"):
                df = read_file(file_path, header=0)
                if df is not None and not df.empty:
                    df['__source_file__'] = file
                    df['Brand'] = brand
                    df['Dealer'] = dealer
                    df['Location'] = location
                    Transfer_Detail.append(df)
                continue

        # ---------- REPORT GEN ----------
        frames_for_oem = []

        # BO LIST → last 90 days; compute transit/T/F/Remark
        if BO_LIST:
            oem = pd.concat(BO_LIST, ignore_index=True)
            # Date parse (supports 2-digit/4-digit year strings)
            oem['PO DATE'] = pd.to_datetime(oem['PO DATE'], errors='coerce')
            cutoff_90 = (datetime.today() - timedelta(days=90)).date()
            oem_work = oem[oem['PO DATE'].dt.date >= cutoff_90].copy()

            # numerics
            for c in ['B/O', 'PROCESSING_ALLOCATION', 'PROCESSING_ON-PICK', 'PROCESSING_ON-PACK',
                      'PROCESSING_PACKED', 'PROCESSING_INVOICE', 'PROCESSING_SHIPPEO', 'QUANTITY_CURRENT']:
                if c in oem_work.columns:
                    oem_work[c] = to_num(oem_work[c])

            oem_work['transit'] = (
                oem_work.get('B/O', 0)
                + oem_work.get('PROCESSING_ALLOCATION', 0)
                + oem_work.get('PROCESSING_ON-PICK', 0)
                + oem_work.get('PROCESSING_ON-PACK', 0)
                + oem_work.get('PROCESSING_PACKED', 0)
                + oem_work.get('PROCESSING_INVOICE', 0)
            )
            oem_work['T/F'] = oem_work.get('QUANTITY_CURRENT', 0).eq(oem_work.get('PROCESSING_SHIPPEO', 0))

            def _remark(r):
                if r['transit'] == 0.0 and bool(r['T/F']) is True:
                    return 'Ok'
                if r['transit'] > 0.0 and bool(r['T/F']) is False:
                    return 'Ok'
                if r['transit'] == 0.0 and bool(r['T/F']) is False:
                    return 'Pls Check'
                return None

            oem_work['Remark'] = oem_work.apply(_remark, axis=1)

            oem_workf = oem_work[['Brand', 'Dealer', 'Location', 'ORDER NO', 'PART NO_CURRENT', 'PO DATE', 'transit', 'Remark']].copy()
            oem_workf.rename(columns={
                'ORDER NO': 'OrderNumber',
                'PART NO_CURRENT': 'PartNumber',
                'PO DATE': 'OrderDate',
                'transit': 'POQty'
            }, inplace=True)
            frames_for_oem.append(oem_workf)

        # Receiving Pending Detail → last 60 days
        if Receving_Pending_Detail:
            rpd = pd.concat(Receving_Pending_Detail, ignore_index=True)
            rpd['ORDER DATE'] = pd.to_datetime(rpd['ORDER DATE'], errors='coerce')
            cutoff_60 = (datetime.today() - timedelta(days=60)).date()
            rpdw = rpd[rpd['ORDER DATE'].dt.date >= cutoff_60].copy()
            rpdw = rpdw[['Brand', 'Dealer', 'Location', 'ORDER NO ', 'PART NO _SUPPLY', 'ORDER DATE', 'ACCEPT QTY', '__source_file__']]
            rpdw.rename(columns={
                'ORDER NO ': 'OrderNumber',
                'PART NO _SUPPLY': 'PartNumber',
                'ORDER DATE': 'OrderDate',
                'ACCEPT QTY': 'POQty',
                '__source_file__': 'Remark'
            }, inplace=True)
            frames_for_oem.append(rpdw)

        # Receiving Today Detail → last 60 days
        if Receving_Today_Detail:
            rtd = pd.concat(Receving_Today_Detail, ignore_index=True)
            rtd['ORDER DATE'] = pd.to_datetime(rtd['ORDER DATE'], errors='coerce')
            cutoff_60 = (datetime.today() - timedelta(days=60)).date()
            rtdw = rtd[rtd['ORDER DATE'].dt.date >= cutoff_60].copy()
            rtdw = rtdw[['Brand', 'Dealer', 'Location', 'ORDER NO ', 'PART NO _SUPPLY', 'ORDER DATE', 'ACCEPT QTY', '__source_file__']]
            rtdw.rename(columns={
                'ORDER NO ': 'OrderNumber',
                'PART NO _SUPPLY': 'PartNumber',
                'ORDER DATE': 'OrderDate',
                'ACCEPT QTY': 'POQty',
                '__source_file__': 'Remark'
            }, inplace=True)
            frames_for_oem.append(rtdw)

        # Save OEM_{...}.xlsx (Hyundai unified)
        # if frames_for_oem:
        #     key_oem = f"OEM_{brand}_{dealer}_{location}.xlsx"
        #     oem_final = pd.concat(frames_for_oem, ignore_index=True)
        #     oem_final['PartNumber']  = oem_final['PartNumber'].astype(str).str.strip().replace('-','').replace('.','')
            
        #     oem_final['OEMInvoiceNo']=''
        #     oem_final['OEMInvoiceDate']=''
        #     oem_final['OEMInvoiceQty']=''
        #     oem_final['OrderDate'] = pd.to_datetime(oem_final['OrderDate'], errors='coerce')
        #     oem_final['OrderDate'] = oem_final['OrderDate'].dt.strftime('%d %b %Y')
        #     oem_c = oem_final[oem_final['Remark']=='Pls Check'][['Location','OrderNumber']].drop_duplicates()
        #     # Preview for UI & for dealerwise ZIP
        #     previews[key_oem] = oem_final.copy()

        #     # Build Excel with two sheets (sheet1: summary of "Pls Check", sheet2: full)
        #     excel_buffer = io.BytesIO()
        #     with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        #         #oem_c = oem_final[oem_final['Remark']=='Pls Check'][['Location','OrderNumber']].drop_duplicates()
        #         oem_c.reset_index(drop=True).to_excel(writer, sheet_name='sheet1', index=False)
        #         oem_final.reset_index(drop=True).to_excel(writer, sheet_name='sheet2', index=False)
        #     files[key_oem] = excel_buffer.getvalue()

        # Save OEM_{...}.xlsx (Hyundai unified)
        if frames_for_oem:
            key_oem = f"OEM_{brand}_{dealer}_{location}.xlsx"
            oem_final = pd.concat(frames_for_oem, ignore_index=True)
        
            # CLEAN: remove - and . safely
            oem_final['PartNumber'] = (
                oem_final['PartNumber'].astype(str).str.strip().str.replace(r'[\-.]', '', regex=True)
            )
        
            oem_final['OEMInvoiceNo'] = ''
            oem_final['OEMInvoiceDate'] = ''
            oem_final['OEMInvoiceQty'] = ''
            oem_final['OrderDate'] = pd.to_datetime(oem_final['OrderDate'], errors='coerce').dt.strftime('%d %b %Y')
        
            # Preview for UI & for dealerwise ZIP
            previews[key_oem] = oem_final.copy()
        
            # Build Excel with two sheets: Summary (Pls Check) + FullData
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                summary = (
                    oem_final.loc[oem_final['Remark'].astype(str).str.strip().str.lower().eq('pls check'),
                                  ['Location', 'OrderNumber']]
                            .drop_duplicates()
                )
                # keep a visible row even if empty (optional)
                if summary.empty:
                    summary = pd.DataFrame([{'Location': '—', 'OrderNumber': 'No "Pls Check" rows'}])
        
                summary.to_excel(writer, sheet_name='Check Order status', index=False)
                oem_final.reset_index(drop=True).to_excel(writer, sheet_name='sheet1', index=False)
        
                # make Summary the active sheet
                writer.book.active = 0
        
            files[key_oem] = excel_buffer.getvalue()

        
        # Save Stock_{...}.xlsx
        if Stock_data:
            key_stock = f"Stock_{brand}_{dealer}_{location}.xlsx"
            stock_final = pd.concat(Stock_data, ignore_index=True)
            previews[key_stock] = stock_final.copy()
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                stock_final.to_excel(writer, index=False, sheet_name="Sheet1")
            files[key_stock] = buf.getvalue()

        # Pending (from Transfer_Detail minimal subset) -> Pending_{...}.xlsx
        if Transfer_Detail:
            tr = pd.concat(Transfer_Detail, ignore_index=True)
            # Only add if expected columns exist
            needed_cols = {'PART NO ?', 'QUANTITY'}
            if needed_cols.issubset(set(tr.columns)):
                tr_Df = tr[['Brand','Dealer','Location','PART NO ?','QUANTITY']].copy()
                tr_Df['PART NO ?'] = tr_Df['PART NO ?'].astype(str).str.strip()
                tr_Df.rename(columns={'PART NO ?':'PartNumber','QUANTITY':'Qty'}, inplace=True)
                key_pending = f"Pending_{brand}_{dealer}_{location}.xlsx"
                previews[key_pending] = tr_Df.copy()
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                    tr_Df.to_excel(writer, index=False, sheet_name="Sheet1")
                files[key_pending] = buf.getvalue()

    # ---------- UI ----------
    if validation_errors:
        st.warning("⚠ Validation issues found:")
        for error in validation_errors:
            st.write(f"- {error}")

    st.success("🎉 Reports generated successfully!")
    st.subheader("📥 Download Reports")

    # Build sections from available file names (files dict is source of truth for downloads)
    report_types = {
        'OEM':      [k for k in files.keys() if k.startswith('OEM_')],
        'Stock':    [k for k in files.keys() if k.startswith('Stock_')],
        'Transfer': [k for k in files.keys() if k.startswith(('Transfer_','Pending_'))],
    }

    for report_type, names in report_types.items():
        if not names:
            continue
        with st.expander(f"📂 {report_type} Reports ({len(names)})", expanded=False):
            for name in names:
                st.markdown(f"### 📄 {name}")

                # Show preview if we have it
                df_preview = previews.get(name)
                if df_preview is not None and not df_preview.empty:
                    st.dataframe(df_preview.head(5))
                else:
                    st.info("No preview available.")

                # Download button (bytes)
                blob = files.get(name)
                if blob:
                    st.download_button(
                        label="⬇ Download Excel",
                        data=blob,
                        file_name=name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_{name}",
                    )
                else:
                    st.warning("⚠ Download content missing for this file.")

    # ---------- Combined ZIP per (report_type, brand, dealer) using previews (DataFrames) ----------
    # grouped_data = defaultdict(list)
    # for file_name, df in previews.items():
    #     if df is None or df.empty:
    #         continue
    #     parts = file_name.replace(".xlsx", "").split("_")
    #     if len(parts) >= 4:
    #         rep, br, dlr = parts[0], parts[1], parts[2]
    #         loc_part = "_".join(parts[3:])
    #         if "Location" not in df.columns:
    #             df = df.copy()
    #             df["Location"] = loc_part
    #         grouped_data[(rep, br, dlr)].append(df)
    #     else:
    #         st.warning(f"❗ Invalid file name format: {file_name}")

    # if grouped_data:
    #     zip_buffer = io.BytesIO()
    #     with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
    #         for (rep, br, dlr), df_list in grouped_data.items():
    #             combined_df = pd.concat(df_list, ignore_index=True)
    #             excel_buffer = io.BytesIO()
    #             with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
    #                 combined_df.to_excel(writer, sheet_name="Sheet1", index=False)
    #             output_filename = f"{rep}_{br}_{dlr}.xlsx"
    #             zipf.writestr(output_filename, excel_buffer.getvalue())

    #     st.download_button(
    #         label="📦 Download Combined Dealer Reports ZIP",
    #         data=zip_buffer.getvalue(),
    #         file_name="Combined_Dealerwise_Reports.zip",
    #         mime="application/zip",
    #     )
    # else:
    #     st.info("ℹ No reports available to download.")
    #     st.warring("Pls check Folder Structure")
    # ---------- Combined ZIP per (report_type, brand, dealer) using previews (DataFrames) ----------
    grouped_data = defaultdict(list)
    for file_name, df in previews.items():
        if df is None or df.empty:
            continue
        parts = file_name.replace(".xlsx", "").split("_")
        if len(parts) >= 4:
            rep, br, dlr = parts[0], parts[1], parts[2]
            loc_part = "_".join(parts[3:])
            if "Location" not in df.columns:
                df = df.copy()
                df["Location"] = loc_part
            grouped_data[(rep, br, dlr)].append(df)
        else:
            st.warning(f"❗ Invalid file name format: {file_name}")
    
    if grouped_data:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for (rep, br, dlr), df_list in grouped_data.items():
                combined_df = pd.concat(df_list, ignore_index=True)
    
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                    if rep == "OEM":
                        summary = (
                            combined_df.loc[combined_df['Remark'].astype(str).str.strip().str.lower().eq('pls check'),
                                            ['Location', 'OrderNumber']]
                                       .drop_duplicates()
                        )
                        if summary.empty:
                            summary = pd.DataFrame([{'Location': '—', 'OrderNumber': 'No "Pls Check" rows'}])
    
                        summary.to_excel(writer, sheet_name="Check Order status", index=False)
                        combined_df.to_excel(writer, sheet_name="sheet1", index=False)
                        writer.book.active = 0
                    else:
                        combined_df.to_excel(writer, sheet_name="Sheet1", index=False)
    
                output_filename = f"{rep}_{br}_{dlr}.xlsx"
                zipf.writestr(output_filename, excel_buffer.getvalue())
    
        st.download_button(
            label="📦 Download Combined Dealer Reports ZIP",
            data=zip_buffer.getvalue(),
            file_name="Combined_Dealerwise_Reports.zip",
            mime="application/zip",
        )
    else:
        st.info("ℹ No reports available to download.")
        st.warning("Pls check Folder Structure")  # (fix typo from st.warring -> st.warning)











