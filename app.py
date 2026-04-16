import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection
import numpy as np
import urllib.parse # Added for Email formatting

# --- CONFIGURATION ---
st.set_page_config(page_title="Mayurank MPS Simulator", layout="wide")
st.markdown("""
    <style>
    .stMetric { border: 1px solid #d3d3d3; padding: 15px; border-radius: 8px; background-color: #ffffff; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    h1 { color: #1E3A8A; }
    .warning-text { color: #dc2626; font-weight: bold; }
    .admin-box { border: 2px solid #dc2626; padding: 20px; border-radius: 10px; background-color: #fef2f2; }
    .legend-box { background-color: #f8fafc; border: 1px solid #cbd5e1; padding: 15px; border-radius: 8px; margin-bottom: 20px; }
    </style>
    """, unsafe_allow_html=True)

# --- CLOUD DB CONNECTION ---
conn = st.connection("gsheets", type=GSheetsConnection)
try:
    master_db = conn.read(worksheet="Backlog", ttl=0).dropna(subset=['Job_ID'])
except Exception as e:
    st.error(f"Database Connection Error. Please verify your Google Sheet has exactly 'Backlog' and 'Archive' tabs. Details: {e}")
    st.stop()

# --- VALIDATION & SCHEDULING LOGIC ---
def process_mrp_and_schedule(df):
    if df.empty: return df, pd.DataFrame()
    
    # 1. MRP (Material Check)
    df['Required_KG'] = (df['Remaining_Qty'] * df['Pack_Size_Grams']) / 1000
    df['Missing_KG'] = df['Required_KG'] - df['Bulk_Stock_KG']
    df['Missing_KG'] = df['Missing_KG'].apply(lambda x: max(0, x))
    df['Status'] = np.where(df['Missing_KG'] > 0, 'BLOCKED', 'READY')
    
    ready_df = df[df['Status'] == 'READY'].copy()
    blocked_df = df[df['Status'] == 'BLOCKED'].copy()
    
    if ready_df.empty: return ready_df, blocked_df
    
    # 2. CUP FILLER RULE VALIDATION
    def check_routing(row):
        line = str(row.get('Preferred_Line', '')).lower()
        if 'cup' in line:
            if row['Pack_Size_Grams'] < 500 or row['Remaining_Qty'] < 300:
                return "❌ INVALID (Move to Manual)"
        return "✅ OK"
    
    ready_df['Routing_Audit'] = ready_df.apply(check_routing, axis=1)
    
    # 3. APPOINTMENT & DISPATCH LOGIC
    blocked_pos = blocked_df['PO_Number'].unique()
    missing_skus_dict = blocked_df.groupby('PO_Number')['SKU'].apply(lambda x: ', '.join(x)).to_dict()
    
    ready_df['Is_Appointment_Fixed'] = ready_df['Is_Appointment_Fixed'].astype(bool)
    ready_df['Strict_PO_Delivery'] = ready_df['Strict_PO_Delivery'].astype(bool)
    
    def get_dispatch_status(row):
        if row['Is_Appointment_Fixed']:
            if row['Strict_PO_Delivery'] and row['PO_Number'] in blocked_pos:
                return f"🚨 APPT AT RISK (Missing: {missing_skus_dict.get(row['PO_Number'], 'Unknown')})"
            else:
                return "📅 APPOINTMENT LOCKED"
        if row['Strict_PO_Delivery'] and row['PO_Number'] in blocked_pos:
            return f"⚠️ STAGE ONLY (Missing: {missing_skus_dict.get(row['PO_Number'], 'Unknown')})"
        return "✅ Clear to Ship"
        
    ready_df['Dispatch_Status'] = ready_df.apply(get_dispatch_status, axis=1)
    
    # 4. PRIORITIZATION SORTING
    margin_weights = {'Very High': 5, 'High': 4, 'Medium': 3, 'Low': 2, 'Very Low': 1}
    ready_df['Margin_Score'] = ready_df['Margin_Class'].map(margin_weights).fillna(1)
    ready_df['Appointment_Date'] = pd.to_datetime(ready_df['Appointment_Date'], errors='coerce')
    ready_df['Delivery_Date'] = pd.to_datetime(ready_df['Delivery_Date'], errors='coerce')
    ready_df['Sort_Date'] = np.where(ready_df['Is_Appointment_Fixed'], ready_df['Appointment_Date'], ready_df['Delivery_Date'])
    
    schedule_df = ready_df.sort_values(
        by=['Is_Appointment_Fixed', 'Is_Urgent', 'Sort_Date', 'Margin_Score'],
        ascending=[False, False, True, False]
    ).reset_index(drop=True)
    
    schedule_df['Sort_Date'] = pd.to_datetime(schedule_df['Sort_Date']).dt.strftime('%Y-%m-%d').fillna("Unscheduled")
    return schedule_df, blocked_df

def calc_utilization(df, cap_per_hr, shift_hrs=10):
    if df.empty or cap_per_hr == 0: return 0, 0
    total_qty = df['Remaining_Qty'].sum()
    hrs_needed = total_qty / cap_per_hr
    util_pct = (hrs_needed / shift_hrs) * 100
    return hrs_needed, util_pct

# --- UI INTERFACE ---
st.title("🌐 Mayurank ERP: 6-Line MPS Simulator")

# --- SIDEBAR CAPACITIES ---
st.sidebar.header("⚙️ 6-Line Hourly Capacities")
cap_dfs_man = st.sidebar.number_input("DFS Manual (Pkts/Hr)", value=1500, step=100)
cap_dfs_cup = st.sidebar.number_input("DFS Cup Filler (Pkts/Hr)", value=2500, step=100)
cap_prs_man = st.sidebar.number_input("PRS Manual (Pkts/Hr)", value=1500, step=100)
cap_prs_cup = st.sidebar.number_input("PRS Cup Filler (Pkts/Hr)", value=2500, step=100)
cap_sugar = st.sidebar.number_input("Sugar FFS (Pkts/Hr)", value=5000, step=500)
cap_horeca = st.sidebar.number_input("HoRECA (Pkts/Hr)", value=2000, step=100)
shift_hours = st.sidebar.slider("Planned Shift Length (Hrs)", min_value=8, max_value=24, value=10)

t1, t2, t3, t4, t5, t6 = st.tabs(["📊 1. Dashboard", "📥 2. Upload", "🗓️ 3. The 6-Line Schedule", "🛒 4. Procurement", "✅ 5. End of Day", "🗄️ 6. System Admin"])

pending_df = master_db[master_db['Remaining_Qty'] > 0].copy()
ready_jobs, blocked_jobs = process_mrp_and_schedule(pending_df)

# --- TAB 1: DASHBOARD & BOOKING RADAR ---
with t1:
    st.subheader("⚙️ Line Load & Utilization Simulator")
    if not ready_jobs.empty:
        lines_data = [
            ("DFS Manual", cap_dfs_man), ("DFS Cup Filler", cap_dfs_cup),
            ("PRS Manual", cap_prs_man), ("PRS Cup Filler", cap_prs_cup),
            ("Sugar FFS", cap_sugar), ("HoRECA", cap_horeca)
        ]
        cols = st.columns(3)
        for idx, (line_name, cap) in enumerate(lines_data):
            line_df = ready_jobs[ready_jobs['Preferred_Line'].astype(str).str.lower() == line_name.lower()]
            hrs, util = calc_utilization(line_df, cap, shift_hours)
            with cols[idx % 3]:
                color = "#dc2626" if util > 100 else ("#d97706" if util > 80 else "#059669")
                st.markdown(f"<div style='border: 1px solid #d3d3d3; padding: 10px; border-radius: 5px; margin-bottom: 10px; border-left: 5px solid {color};'><h4>{line_name}</h4><p><b>Load:</b> {hrs:.1f} Hrs</p><p><b>Utilization:</b> <span style='color: {color}; font-weight: bold;'>{util:.1f}%</span></p></div>", unsafe_allow_html=True)
                
        st.divider()
        
        st.markdown("### 📞 Appointment Booking Radar")
        st.write("These POs do not have a fixed appointment yet. Here is whether it is safe to call the client to lock a date.")
        
        unbooked_df = pending_df[~pending_df['Is_Appointment_Fixed'].astype(bool)].copy()
        
        if not unbooked_df.empty:
            po_group = unbooked_df.groupby('PO_Number').agg({
                'Delivery_Date': 'min', 
                'Missing_KG': 'sum' 
            }).reset_index()
            
            def booking_advice(row):
                if row['Missing_KG'] > 0: return "❌ DO NOT BOOK (Missing Materials)"
                return "✅ SAFE TO BOOK (-48 Hrs from Expiry)"
                
            po_group['Booking_Advice'] = po_group.apply(booking_advice, axis=1)
            
            def style_advice(row):
                if "DO NOT" in row['Booking_Advice']: return ['color: #dc2626; font-weight: bold'] * len(row)
                return ['color: #059669; font-weight: bold'] * len(row)
            
            st.dataframe(po_group[['PO_Number', 'Delivery_Date', 'Booking_Advice']].style.apply(style_advice, axis=1), use_container_width=True)
        else:
            st.info("All current POs already have fixed appointments.")
            
    else:
        st.info("No active jobs. Upload a plan.")

# --- TAB 2: UPLOAD ---
with t2:
    st.subheader("Upload Planned Roster")
    uploaded_file = st.file_uploader("Upload Orders File (CSV/Excel)", type=['csv', 'xlsx'])
    if uploaded_file and st.button("Simulate & Save"):
        new_data = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('xlsx') else pd.read_csv(uploaded_file)
        new_data['Remaining_Qty'] = new_data['Ordered_Qty']
        updated_db = pd.concat([master_db, new_data], ignore_index=True).drop_duplicates(subset=['Job_ID'], keep='last')
        conn.update(worksheet="Backlog", data=updated_db)
        st.success("Plan updated successfully!")
        st.rerun()

# --- TAB 3: THE 6-LINE SCHEDULE (WITH LEGEND) ---
with t3:
    if not ready_jobs.empty:
        st.markdown("""
        <div class="legend-box">
            <h4 style='margin-top:0px;'>🎨 Floor Supervisor Color Guide</h4>
            <p>Always process jobs from Top to Bottom. Look at the row color to know what to do with the physical boxes.</p>
            <ul style="list-style-type:none; padding-left:0;">
                <li>🟦 <b>BLUE (LOCKED):</b> Appointment is booked. Do not stop running this job until it is 100% finished. Send immediately to dispatch.</li>
                <li>⬜ <b>WHITE (NORMAL):</b> Standard priority. Pack it and send to dispatch.</li>
                <li>🟨 <b>YELLOW (STAGE ONLY):</b> Pack it, but leave the boxes in the corner. Do NOT send to dispatch (waiting on missing items for the same PO).</li>
                <li>🟥 <b>RED (INVALID):</b> Planner error (e.g. 100g on a Cup Filler). <b>SKIP IT entirely.</b> Do not run this job on this machine.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        display_cols = ['PO_Number', 'Job_ID', 'SKU', 'Remaining_Qty', 'Routing_Audit', 'Dispatch_Status', 'Sort_Date']
        
        def style_rows(row):
            styles = [''] * len(row)
            if "INVALID" in row['Routing_Audit']: styles = ['background-color: #fecdd3'] * len(row) 
            elif "AT RISK" in row['Dispatch_Status']: styles = ['background-color: #fef08a'] * len(row) 
            elif "STAGE ONLY" in row['Dispatch_Status']: styles = ['background-color: #fef08a'] * len(row) 
            elif "LOCKED" in row['Dispatch_Status']: styles = ['background-color: #dbeafe'] * len(row) 
            return styles

        lines_to_display = ["DFS Manual", "DFS Cup Filler", "PRS Manual", "PRS Cup Filler", "Sugar FFS", "HoRECA"]
        
        for line in lines_to_display:
            with st.expander(f"🏭 {line} Schedule", expanded=True):
                line_df = ready_jobs[ready_jobs['Preferred_Line'].astype(str).str.lower() == line.lower()].copy()
                if not line_df.empty:
                    st.dataframe(line_df[display_cols].style.apply(style_rows, axis=1), use_container_width=True)
                else:
                    st.info(f"No jobs assigned to {line}.")
    else:
        st.warning("No Clear-to-Build jobs available.")

# --- TAB 4: PROCUREMENT (WITH EMAIL BUTTON) ---
with t4:
    st.subheader("🛒 S.O.S. Procurement Dashboard")
    st.write("These jobs have been stripped from the production schedule because the required bulk stock is missing.")
    if not blocked_jobs.empty:
        st.error(f"WARNING: {len(blocked_jobs)} jobs are blocked by material shortages.")
        
        # 1. Show the list
        st.markdown("#### Aggregated Buying List:")
        buy_list = blocked_jobs.groupby('SKU')['Missing_KG'].sum().reset_index().sort_values(by='Missing_KG', ascending=False)
        st.dataframe(buy_list.style.format({'Missing_KG': "{:.1f} kg"}), use_container_width=True)
        
        # 2. Email Button Logic
        st.markdown("### 📧 Notify Purchasing")
        st.write("Click the button below to instantly draft an email to the procurement team with these exact shortages.")
        
        email_to = "purchase@mayurankfoods.com"
        date_str = pd.Timestamp.now().strftime("%Y-%m-%d")
        email_subject = urllib.parse.quote(f"URGENT: Mayurank Raw Material Shortages - {date_str}")
        
        body_text = f"Hello Procurement Team,\n\nPlease arrange the following missing raw materials immediately to unblock the factory floor for {date_str}:\n\n"
        for _, row in buy_list.iterrows():
            body_text += f"• {row['SKU']}: {row['Missing_KG']:.1f} kg\n"
        body_text += "\nThank you,\nProduction Planning via Mayurank ERP"
        
        email_body = urllib.parse.quote(body_text)
        mailto_link = f"mailto:{email_to}?subject={email_subject}&body={email_body}"
        
        st.link_button("📤 Email purchase@mayurankfoods.com", mailto_link, type="primary")
        
        st.divider()
        
        # 3. Blocked Details
        st.markdown("#### Blocked Job Details (Why we need it):")
        st.dataframe(blocked_jobs[['PO_Number', 'SKU', 'Missing_KG', 'Delivery_Date']], use_container_width=True)
    else:
        st.success("All pending jobs have sufficient raw materials!")

# --- TAB 5: END OF DAY ACTUALS ---
with t5:
    st.subheader("✅ End of Day Actuals")
    st.write("Download the template, fill in actual quantities produced today, and upload to update the master backlog.")
    
    if not pending_df.empty:
        template_df = pending_df[['Job_ID', 'SKU', 'Preferred_Line', 'Remaining_Qty']].copy()
        template_df['Actual_Produced'] = ""
        
        st.download_button(
            label="📥 Download Today's EOD Template", 
            data=template_df.to_csv(index=False).encode('utf-8'), 
            file_name="EOD_Template.csv", 
            mime="text/csv"
        )
        st.divider()
        
        eod_file = st.file_uploader("Upload Completed EOD Sheet", type=['csv', 'xlsx'], key="eod_upload")
        if eod_file and st.button("Process Bulk EOD Data"):
            eod_data = pd.read_csv(eod_file) if eod_file.name.endswith('.csv') else pd.read_excel(eod_file)
            
            if 'Job_ID' not in eod_data.columns or 'Actual_Produced' not in eod_data.columns:
                st.error("Upload failed: Missing 'Job_ID' or 'Actual_Produced' columns.")
            else:
                updated_count = 0
                for index, row in eod_data.iterrows():
                    job_id = str(row['Job_ID']).strip()
                    actual = pd.to_numeric(row['Actual_Produced'], errors='coerce')
                    
                    if not pd.isna(actual) and job_id in master_db['Job_ID'].astype(str).values:
                        expected = master_db.loc[master_db['Job_ID'].astype(str) == job_id, 'Remaining_Qty'].values[0]
                        master_db.loc[master_db['Job_ID'].astype(str) == job_id, 'Remaining_Qty'] = max(0, expected - actual)
                        updated_count += 1
                
                conn.update(worksheet="Backlog", data=master_db)
                st.success(f"Successfully processed {updated_count} jobs! Database updated.")
                st.rerun()
    else:
        st.success("All jobs are completed! No EOD processing required.")

# --- TAB 6: SYSTEM ADMIN (ARCHIVE) ---
with t6:
    st.subheader("🗄️ Database Hygiene")
    st.write("Over time, completed jobs will slow down the application. Use this routine at the end of the month to permanently move completed jobs (Remaining Qty = 0) out of the active database and into your historical Archive tab.")
    
    completed_jobs = master_db[master_db['Remaining_Qty'] <= 0].copy()
    
    if not completed_jobs.empty:
        st.markdown("<div class='admin-box'>", unsafe_allow_html=True)
        st.warning(f"**Action Required:** You have {len(completed_jobs)} fully completed jobs cluttering the active database.")
        
        if st.button("🚨 Execute Monthly Archive Routine"):
            try:
                archive_db = conn.read(worksheet="Archive", ttl=0).dropna(subset=['Job_ID'])
                updated_archive = pd.concat([archive_db, completed_jobs], ignore_index=True)
            except Exception as e:
                # If Archive tab is completely empty, just use the completed jobs
                updated_archive = completed_jobs
                
            new_active_backlog = master_db[master_db['Remaining_Qty'] > 0]
            
            # Push updates to both sheets simultaneously 
            conn.update(worksheet="Archive", data=updated_archive)
            conn.update(worksheet="Backlog", data=new_active_backlog)
            
            st.success(f"Success! {len(completed_jobs)} jobs have been permanently moved to the Archive sheet.")
            st.rerun()
            
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.success("✅ Your database is clean! There are no fully completed jobs to archive right now.")
