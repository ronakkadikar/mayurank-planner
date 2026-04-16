import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection
import numpy as np

# --- CONFIGURATION ---
st.set_page_config(page_title="Mayurank MPS Simulator", layout="wide")
st.markdown("""
    <style>
    .stMetric { border: 1px solid #d3d3d3; padding: 15px; border-radius: 8px; background-color: #ffffff; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    h1 { color: #1E3A8A; }
    .warning-text { color: #dc2626; font-weight: bold; }
    .admin-box { border: 2px solid #dc2626; padding: 20px; border-radius: 10px; background-color: #fef2f2; }
    </style>
    """, unsafe_allow_html=True)

# --- CLOUD DB CONNECTION ---
conn = st.connection("gsheets", type=GSheetsConnection)
try:
    master_db = conn.read(worksheet="Backlog", ttl=0).dropna(subset=['Job_ID'])
except Exception as e:
    st.error(f"Database Connection Error: Could not connect to Google Sheets. Check your Secrets and Tab names (Backlog, Archive). Details: {e}")
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
                return f"🚨 APPT AT RISK (Missing: {missing_skus_dict[row['PO_Number']]})"
            else:
                return "📅 APPOINTMENT LOCKED"
                
        if row['Strict_PO_Delivery'] and row['PO_Number'] in blocked_pos:
            return f"⚠️ STAGE ONLY (Missing: {missing_skus_dict[row['PO_Number']]})"
        return "✅ Clear to Ship"
        
    ready_df['Dispatch_Status'] = ready_df.apply(get_dispatch_status, axis=1)
    
    # 4. SORTING: APPOINTMENTS REIGN SUPREME
    margin_weights = {'Very High': 5, 'High': 4, 'Medium': 3, 'Low': 2, 'Very Low': 1}
    ready_df['Margin_Score'] = ready_df['Margin_Class'].map(margin_weights).fillna(1)
    
    # Parse dates safely
    ready_df['Appointment_Date'] = pd.to_datetime(ready_df['Appointment_Date'], errors='coerce')
    ready_df['Delivery_Date'] = pd.to_datetime(ready_df['Delivery_Date'], errors='coerce')
    ready_df['Sort_Date'] = np.where(ready_df['Is_Appointment_Fixed'], ready_df['Appointment_Date'], ready_df['Delivery_Date'])
    
    # Prioritize: Appointments -> Urgent -> Date -> Margin
    schedule_df = ready_df.sort_values(
        by=['Is_Appointment_Fixed', 'Is_Urgent', 'Sort_Date', 'Margin_Score'],
        ascending=[False, False, True, False]
    ).reset_index(drop=True)
    
    # Safely convert Sort_Date back to string for display without crashing if empty/NaT
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

# --- CAPACITIES (6 LINES) ---
st.sidebar.header("⚙️ 6-Line Hourly Capacities")
cap_dfs_man = st.sidebar.number_input("DFS Manual (Pkts/Hr)", value=1500, step=100)
cap_dfs_cup = st.sidebar.number_input("DFS Cup Filler (Pkts/Hr)", value=2500, step=100)
cap_prs_man = st.sidebar.number_input("PRS Manual (Pkts/Hr)", value=1500, step=100)
cap_prs_cup = st.sidebar.number_input("PRS Cup Filler (Pkts/Hr)", value=2500, step=100)
cap_sugar = st.sidebar.number_input("Sugar FFS (Pkts/Hr)", value=5000, step=500)
cap_horeca = st.sidebar.number_input("HoRECA (Pkts/Hr)", value=2000, step=100)

shift_hours = st.sidebar.slider("Planned Shift Length (Hrs)", min_value=8, max_value=24, value=10)

# Create the 6 Tabs
t1, t2, t3, t4, t5, t6 = st.tabs(["📊 1. Utilization Dashboard", "📥 2. Upload / Re-Plan", "🗓️ 3. The 6-Line Schedule", "🛒 4. S.O.S Procurement", "✅ 5. End of Day", "🗄️ 6. System Admin"])

pending_df = master_db[master_db['Remaining_Qty'] > 0].copy()
ready_jobs, blocked_jobs = process_mrp_and_schedule(pending_df)

# --- TAB 1: UTILIZATION DASHBOARD ---
with t1:
    st.subheader("⚙️ Line Load & Utilization Simulator")
    st.write(f"Based on a **{shift_hours}-hour** shift. If utilization is over 100%, the line will fail today. Re-route jobs in your Excel file and upload again.")
    
    if not ready_jobs.empty:
        # Calculate loads for all 6 lines
        lines_data = [
            ("DFS Manual", cap_dfs_man), ("DFS Cup Filler", cap_dfs_cup),
            ("PRS Manual", cap_prs_man), ("PRS Cup Filler", cap_prs_cup),
            ("Sugar FFS", cap_sugar), ("HoRECA", cap_horeca)
        ]
        
        cols = st.columns(3)
        for idx, (line_name, cap) in enumerate(lines_data):
            line_df = ready_jobs[ready_jobs['Preferred_Line'].str.lower() == line_name.lower()]
            hrs, util = calc_utilization(line_df, cap, shift_hours)
            
            with cols[idx % 3]:
                if util > 100: color = "#dc2626" # Red
                elif util > 80: color = "#d97706" # Yellow
                else: color = "#059669" # Green
                
                st.markdown(f"""
                <div style='border: 1px solid #d3d3d3; padding: 10px; border-radius: 5px; margin-bottom: 10px; border-left: 5px solid {color};'>
                    <h4>{line_name}</h4>
                    <p><b>Load:</b> {hrs:.1f} Hrs</p>
                    <p><b>Utilization:</b> <span style='color: {color}; font-weight: bold;'>{util:.1f}%</span></p>
                </div>
                """, unsafe_allow_html=True)
                
        st.divider()
        st.markdown("### 🚨 Appointment Risk Radar")
        at_risk = ready_jobs[ready_jobs['Dispatch_Status'].str.contains("APPT AT RISK")]
        if not at_risk.empty:
            st.error(f"You have **{len(at_risk)}** jobs scheduled for a Strict Appointment today that are missing raw materials. Alert Procurement IMMEDIATELY.")
            st.dataframe(at_risk[['PO_Number', 'SKU', 'Sort_Date', 'Dispatch_Status']], use_container_width=True)
        else:
            st.success("No appointments are currently at risk due to material shortages.")
    else:
        st.info("No active jobs. Upload a plan.")

# --- TAB 2: UPLOAD ---
with t2:
    st.subheader("Upload Planned Roster")
    st.write("Ensure your columns exactly match the required format before uploading.")
    uploaded_file = st.file_uploader("Upload Orders File (CSV/Excel)", type=['csv', 'xlsx'])
    if uploaded_file and st.button("Simulate & Save"):
        new_data = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('xlsx') else pd.read_csv(uploaded_file)
        new_data['Remaining_Qty'] = new_data['Ordered_Qty']
        
        updated_db = pd.concat([master_db, new_data], ignore_index=True)
        updated_db = updated_db.drop_duplicates(subset=['Job_ID'], keep='last')
        
        conn.update(worksheet="Backlog", data=updated_db)
        st.success("Plan updated successfully! Check the Utilization Dashboard.")
        st.rerun()

# --- TAB 3: THE 6-LINE SCHEDULE ---
with t3:
    if not ready_jobs.empty:
        st.write("Hand this schedule to the floor supervisor. Rows highlighted in red have routing errors. Yellow rows are for staging only.")
        display_cols = ['PO_Number', 'Job_ID', 'SKU', 'Remaining_Qty', 'Routing_Audit', 'Dispatch_Status', 'Sort_Date']
        
        def style_rows(row):
            styles = [''] * len(row)
            if "INVALID" in row['Routing_Audit']: styles = ['background-color: #fecdd3'] * len(row) # Light Red
            elif "AT RISK" in row['Dispatch_Status']: styles = ['background-color: #fef08a'] * len(row) # Yellow
            elif "STAGE ONLY" in row['Dispatch_Status']: styles = ['background-color: #fef08a'] * len(row) # Yellow
            elif "LOCKED" in row['Dispatch_Status']: styles = ['background-color: #dbeafe'] * len(row) # Blue
            return styles

        lines_to_display = ["DFS Manual", "DFS Cup Filler", "PRS Manual", "PRS Cup Filler", "Sugar FFS", "HoRECA"]
        
        for line in lines_to_display:
            with st.expander(f"🏭 {line} Schedule", expanded=True):
                # Ensure we handle nulls or missing 'Preferred_Line' gracefully
                line_df = ready_jobs[ready_jobs['Preferred_Line'].astype(str).str.lower() == line.lower()].copy()
                if not line_df.empty:
                    st.dataframe(line_df[display_cols].style.apply(style_rows, axis=1), use_container_width=True)
                else:
                    st.info(f"No jobs assigned to {line}.")
    else:
        st.warning("No Clear-to-Build jobs available. Please check procurement shortages.")

# --- TAB 4: PROCUREMENT ---
with t4:
    st.subheader("🛒 S.O.S. Procurement Dashboard")
    st.write("These jobs have been stripped from the production schedule because the required bulk stock is missing.")
    if not blocked_jobs.empty:
        st.error(f"WARNING: {len(blocked_jobs)} jobs are blocked by material shortages.")
        
        st.markdown("#### Aggregated Buying List:")
        buy_list = blocked_jobs.groupby('SKU')['Missing_KG'].sum().reset_index().sort_values(by='Missing_KG', ascending=False)
        st.dataframe(buy_list.style.format({'Missing_KG': "{:.1f} kg"}), use_container_width=True)
        
        st.markdown("#### Blocked Job Details:")
        st.dataframe(blocked_jobs[['PO_Number', 'SKU', 'Missing_KG', 'Delivery_Date']], use_container_width=True)
    else:
        st.success("All pending jobs have sufficient raw materials!")

# --- TAB 5: END OF DAY (BULK UPLOAD) ---
with t5:
    st.subheader("✅ End of Day Actuals")
    st.write("Download the template, fill in actual quantities produced, and upload to update the master backlog.")
    if not pending_df.empty:
        # 1. Download Template
        template_df = pending_df[['Job_ID', 'SKU', 'Preferred_Line', 'Remaining_Qty']].copy()
        template_df['Actual_Produced'] = ""
        
        st.download_button(
            label="📥 Download Today's EOD Template", 
            data=template_df.to_csv(index=False).encode('utf-8'), 
            file_name="EOD_Template.csv", 
            mime="text/csv"
        )
        st.divider()
        
        # 2. Upload Actuals
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
                    if pd.isna(actual): actual = 0
                        
                    if job_id in master_db['Job_ID'].astype(str).values:
                        expected = master_db.loc[master_db['Job_ID'].astype(str) == job_id, 'Remaining_Qty'].values[0]
                        new_remaining = max(0, expected - actual)
                        master_db.loc[master_db['Job_ID'].astype(str) == job_id, 'Remaining_Qty'] = new_remaining
                        updated_count += 1
                
                conn.update(worksheet="Backlog", data=master_db)
                st.success(f"Successfully processed {updated_count} jobs! Cloud updated.")
                st.rerun()
    else:
        st.success("All jobs are completed!")

# --- TAB 6: SYSTEM ADMIN (ARCHIVE) ---
with t6:
    st.subheader("🗄️ Database Hygiene")
    st.write("Move fully completed jobs (Remaining_Qty = 0) to the Archive sheet to keep the active database fast.")
    
    completed_jobs = master_db[master_db['Remaining_Qty'] <= 0].copy()
    if not completed_jobs.empty:
        st.markdown("<div class='admin-box'>", unsafe_allow_html=True)
        st.warning(f"**Action Required:** You have {len(completed_jobs)} fully completed jobs to archive.")
        
        if st.button("🚨 Execute Archive Routine"):
            try:
                archive_db = conn.read(worksheet="Archive", ttl=0).dropna(subset=['Job_ID'])
                updated_archive = pd.concat([archive_db, completed_jobs], ignore_index=True)
            except Exception:
                updated_archive = completed_jobs
                
            new_active_backlog = master_db[master_db['Remaining_Qty'] > 0]
            
            # Update both Google Sheet tabs
            conn.update(worksheet="Archive", data=updated_archive)
            conn.update(worksheet="Backlog", data=new_active_backlog)
            
            st.success(f"Success! {len(completed_jobs)} jobs archived.")
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.success("✅ Database is clean. No jobs with 0 Remaining Qty found.")
