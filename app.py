import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection
import numpy as np
import urllib.parse

# --- CONFIGURATION ---
st.set_page_config(page_title="Mayurank MPS Simulator", layout="wide")
st.markdown("""
    <style>
    .stMetric { border: 1px solid #d3d3d3; padding: 15px; border-radius: 8px; background-color: #ffffff; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    h1 { color: #1E3A8A; }
    .warning-text { color: #dc2626; font-weight: bold; }
    .admin-box { border: 2px solid #dc2626; padding: 20px; border-radius: 10px; background-color: #fef2f2; }
    .legend-box { background-color: #f8fafc; border: 1px solid #cbd5e1; padding: 15px; border-radius: 8px; margin-bottom: 20px; }
    .custom-email-btn { background-color: #1E3A8A; color: white; padding: 10px 20px; border: none; border-radius: 6px; cursor: pointer; font-size: 16px; font-weight: bold; text-decoration: none; display: inline-block; }
    .custom-email-btn:hover { background-color: #1e40af; }
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
    
    # 2. DEPARTMENT & ROUTING AUDIT
    def check_routing(row):
        line = str(row.get('Preferred_Line', '')).lower()
        cat = str(row.get('Category', '')).lower()
        
        # Dept Cross-Contamination Check
        dfs_cats = ['spice', 'dry fruit', 'flour', 'nut']
        prs_cats = ['pulse', 'rice', 'sugar']
        
        if any(c in cat for c in dfs_cats) and ('prs' in line or 'sugar' in line):
            return "❌ INVALID (DFS Item on PRS Line)"
        if any(c in cat for c in prs_cats) and 'dfs' in line:
            return "❌ INVALID (PRS Item on DFS Line)"
            
        # Cup Filler Check
        if 'cup' in line:
            if row['Pack_Size_Grams'] < 500 or row['Remaining_Qty'] < 300:
                return "❌ INVALID (<500g on Cup Filler)"
                
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

# --- DYNAMIC PER-LINE TIMING ---
def assign_timing(df, hourly_cap, shift_hrs_today):
    if df.empty: 
        df['Shift_Window'] = ""
        return df
    if hourly_cap == 0 or shift_hrs_today == 0:
        df['Shift_Window'] = "🔴 PUSHED (Line Closed)"
        return df
        
    df['Est_Run_Time_Hrs'] = df['Remaining_Qty'] / hourly_cap
    df['Cumulative_Hrs'] = df['Est_Run_Time_Hrs'].cumsum()
    
    def get_shift(x):
        if x <= 8.0 and x <= shift_hrs_today: 
            return "🟢 Standard (0-8h)"
        elif x > 8.0 and x <= shift_hrs_today: 
            return f"🟡 Overtime (8-{shift_hrs_today}h)"
        else: 
            return "🔴 PUSHED (Tomorrow)"
        
    df['Shift_Window'] = df['Cumulative_Hrs'].apply(get_shift)
    return df

def calc_utilization(df, cap_per_hr, shift_hrs):
    if df.empty or cap_per_hr == 0 or shift_hrs == 0: return 0, 0
    total_qty = df['Remaining_Qty'].sum()
    hrs_needed = total_qty / cap_per_hr
    util_pct = (hrs_needed / shift_hrs) * 100
    return hrs_needed, util_pct

# --- UI INTERFACE ---
st.title("🌐 Mayurank ERP: 6-Line MPS Simulator")

# --- SIDEBAR: DYNAMIC CONTROLS ---
st.sidebar.header("⚙️ Theoretical Capacities")
cap_dfs_man = st.sidebar.number_input("DFS Manual (Pkts/Hr)", value=3750, step=100)
cap_dfs_cup = st.sidebar.number_input("DFS Cup Filler (Pkts/Hr)", value=0, step=100)
cap_prs_man = st.sidebar.number_input("PRS Manual (Pkts/Hr)", value=1250, step=100)
cap_prs_cup = st.sidebar.number_input("PRS Cup Filler (Pkts/Hr)", value=0, step=100)
cap_sugar = st.sidebar.number_input("Sugar FFS (Pkts/Hr)", value=1100, step=100)
cap_horeca = st.sidebar.number_input("HoRECA (Pkts/Hr)", value=200, step=50)

st.sidebar.divider()

st.sidebar.header("🕒 Today's Labor Allocation")
st.sidebar.write("*8.0 = No OT. 11.0 = Max OT. 0 = Closed.*")
hrs_dfs_man = st.sidebar.number_input("DFS Manual Hrs", value=8.0, step=0.5, max_value=11.0, min_value=0.0)
hrs_dfs_cup = st.sidebar.number_input("DFS Cup Filler Hrs", value=0.0, step=0.5, max_value=11.0, min_value=0.0)
hrs_prs_man = st.sidebar.number_input("PRS Manual Hrs", value=8.0, step=0.5, max_value=11.0, min_value=0.0)
hrs_prs_cup = st.sidebar.number_input("PRS Cup Filler Hrs", value=0.0, step=0.5, max_value=11.0, min_value=0.0)
hrs_sugar = st.sidebar.number_input("Sugar FFS Hrs", value=8.0, step=0.5, max_value=11.0, min_value=0.0)
hrs_horeca = st.sidebar.number_input("HoRECA Hrs", value=8.0, step=0.5, max_value=11.0, min_value=0.0)

line_configs = {
    "DFS Manual": {"cap": cap_dfs_man, "hrs": hrs_dfs_man},
    "DFS Cup Filler": {"cap": cap_dfs_cup, "hrs": hrs_dfs_cup},
    "PRS Manual": {"cap": cap_prs_man, "hrs": hrs_prs_man},
    "PRS Cup Filler": {"cap": cap_prs_cup, "hrs": hrs_prs_cup},
    "Sugar FFS": {"cap": cap_sugar, "hrs": hrs_sugar},
    "HoRECA": {"cap": cap_horeca, "hrs": hrs_horeca}
}

t1, t2, t3, t4, t5, t6 = st.tabs(["📊 1. Dashboard", "📥 2. Upload", "🗓️ 3. The 6-Line Schedule", "🛒 4. Procurement", "✅ 5. End of Day", "🗄️ 6. System Admin"])

pending_df = master_db[master_db['Remaining_Qty'] > 0].copy()
ready_jobs, blocked_jobs = process_mrp_and_schedule(pending_df)

# --- TAB 1: DASHBOARD ---
with t1:
    st.subheader("⚙️ Line Load & Overtime Simulator")
    if not ready_jobs.empty:
        cols = st.columns(3)
        for idx, (line_name, config) in enumerate(line_configs.items()):
            cap = config['cap']
            shift_hrs = config['hrs']
            
            line_df = ready_jobs[ready_jobs['Preferred_Line'].astype(str).str.lower() == line_name.lower()]
            hrs_needed, util = calc_utilization(line_df, cap, shift_hrs)
            
            if shift_hrs == 0 or cap == 0:
                color = "#94a3b8" # Grey
                status_text = "LINE CLOSED (Cap=0 or Hrs=0)"
            elif hrs_needed <= 8.0 and hrs_needed <= shift_hrs:
                color = "#059669" # Green
                status_text = "Standard Shift"
            elif hrs_needed <= shift_hrs:
                color = "#d97706" # Yellow
                status_text = f"OT Authorized"
            else:
                color = "#dc2626" # Red
                status_text = f"OVERLOADED! Exceeds {shift_hrs}h"
                
            with cols[idx % 3]:
                st.markdown(f"<div style='border: 1px solid #d3d3d3; padding: 10px; border-radius: 5px; margin-bottom: 10px; border-left: 5px solid {color};'><h4>{line_name}</h4><p><b>Load:</b> {hrs_needed:.1f} Hrs</p><p><b>Status:</b> <span style='color: {color}; font-weight: bold;'>{status_text}</span></p><p style='font-size: 12px; color: gray; margin: 0;'>Labor Available Today: {shift_hrs}h</p></div>", unsafe_allow_html=True)
                
        st.divider()
        
        st.markdown("### 📞 Appointment Booking Radar")
        unbooked_df = pending_df[~pending_df['Is_Appointment_Fixed'].astype(bool)].copy()
        
        if not unbooked_df.empty:
            po_group = unbooked_df.groupby('PO_Number').agg({'Delivery_Date': 'min', 'Missing_KG': 'sum'}).reset_index()
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

# --- TAB 3: THE 6-LINE SCHEDULE ---
with t3:
    if not ready_jobs.empty:
        st.markdown("""
        <div class="legend-box">
            <h4 style='margin-top:0px;'>🎨 Floor Supervisor Guide</h4>
            <ul style="list-style-type:none; padding-left:0;">
                <li>🟦 <b>BLUE (LOCKED):</b> Appointment booked. Do not stop until 100% finished.</li>
                <li>🟨 <b>YELLOW (STAGE):</b> Pack it, but leave boxes in staging area (waiting on missing PO items).</li>
                <li>🟥 <b>RED (INVALID):</b> Planner error (e.g., Wrong Dept). <b>SKIP IT entirely.</b></li>
                <li>🔘 <b>GREY (PUSHED):</b> Shift is over. Ignore these jobs until tomorrow's shift.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        display_cols = ['PO_Number', 'Job_ID', 'SKU', 'Remaining_Qty', 'Routing_Audit', 'Dispatch_Status', 'Shift_Window']
        
        def style_rows(row):
            styles = [''] * len(row)
            if "INVALID" in row.get('Routing_Audit', ''): styles = ['background-color: #fecdd3'] * len(row) 
            elif "AT RISK" in row.get('Dispatch_Status', ''): styles = ['background-color: #fef08a'] * len(row) 
            elif "STAGE ONLY" in row.get('Dispatch_Status', ''): styles = ['background-color: #fef08a'] * len(row) 
            elif "LOCKED" in row.get('Dispatch_Status', ''): styles = ['background-color: #dbeafe'] * len(row) 
            
            if "PUSHED" in row.get('Shift_Window', ''): styles = ['background-color: #f1f5f9; color: #94a3b8'] * len(row)
            return styles

        for line, config in line_configs.items():
            with st.expander(f"🏭 {line} Schedule", expanded=True):
                line_df = ready_jobs[ready_jobs['Preferred_Line'].astype(str).str.lower() == line.lower()].copy()
                if not line_df.empty:
                    line_df = assign_timing(line_df, config['cap'], config['hrs'])
                    st.dataframe(line_df[display_cols].style.apply(style_rows, axis=1), use_container_width=True)
                else:
                    st.info(f"No jobs assigned to {line}.")
    else:
        st.warning("No Clear-to-Build jobs available.")

# --- TAB 4: PROCUREMENT ---
with t4:
    st.subheader("🛒 S.O.S. Procurement Dashboard")
    if not blocked_jobs.empty:
        st.error(f"WARNING: {len(blocked_jobs)} jobs are blocked by material shortages.")
        buy_list = blocked_jobs.groupby('SKU')['Missing_KG'].sum().reset_index().sort_values(by='Missing_KG', ascending=False)
        st.dataframe(buy_list.style.format({'Missing_KG': "{:.1f} kg"}), use_container_width=True)
        
        email_to = "purchase@mayurankfoods.com,somraj.mukherjee@ofbusiness.in,amit.goswami@ofbusiness.in"
        date_str = pd.Timestamp.now().strftime("%Y-%m-%d")
        email_subject = urllib.parse.quote(f"URGENT: Mayurank Raw Material Shortages - {date_str}")
        body_text = f"Hello Procurement Team,\n\nPlease arrange the following missing raw materials immediately to unblock the factory floor for {date_str}:\n\n"
        for _, row in buy_list.iterrows():
            body_text += f"• {row['SKU']}: {row['Missing_KG']:.1f} kg\n"
        body_text += "\nThank you,\nProduction Planning"
        
        mailto_link = f"mailto:{email_to}?subject={email_subject}&body={urllib.parse.quote(body_text)}"
        st.markdown(f'<a href="{mailto_link}"><button class="custom-email-btn">📤 Email Procurement Team</button></a>', unsafe_allow_html=True)
        
        st.divider()
        st.dataframe(blocked_jobs[['PO_Number', 'SKU', 'Missing_KG', 'Delivery_Date']], use_container_width=True)
    else:
        st.success("All pending jobs have sufficient raw materials!")

# --- TAB 5: END OF DAY ACTUALS ---
with t5:
    st.subheader("✅ End of Day Actuals")
    if not pending_df.empty:
        template_df = pending_df[['Job_ID', 'SKU', 'Preferred_Line', 'Remaining_Qty']].copy()
        template_df['Actual_Produced'] = ""
        st.download_button("📥 Download Today's EOD Template", data=template_df.to_csv(index=False).encode('utf-8'), file_name="EOD_Template.csv", mime="text/csv")
        st.divider()
        eod_file = st.file_uploader("Upload Completed EOD Sheet", type=['csv', 'xlsx'], key="eod_upload")
        if eod_file and st.button("Process Bulk EOD Data"):
            eod_data = pd.read_csv(eod_file) if eod_file.name.endswith('.csv') else pd.read_excel(eod_file)
            if 'Job_ID' not in eod_data.columns or 'Actual_Produced' not in eod_data.columns:
                st.error("Upload failed: Missing columns.")
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
                st.success(f"Processed {updated_count} jobs! Database updated.")
                st.rerun()
    else:
        st.success("All jobs are completed!")

# --- TAB 6: SYSTEM ADMIN ---
with t6:
    st.subheader("🗄️ Database Hygiene")
    completed_jobs = master_db[master_db['Remaining_Qty'] <= 0].copy()
    if not completed_jobs.empty:
        st.markdown("<div class='admin-box'>", unsafe_allow_html=True)
        st.warning(f"**Action Required:** {len(completed_jobs)} completed jobs ready to archive.")
        if st.button("🚨 Execute Monthly Archive Routine"):
            try:
                archive_db = conn.read(worksheet="Archive", ttl=0).dropna(subset=['Job_ID'])
                updated_archive = pd.concat([archive_db, completed_jobs], ignore_index=True)
            except:
                updated_archive = completed_jobs
            new_active_backlog = master_db[master_db['Remaining_Qty'] > 0]
            conn.update(worksheet="Archive", data=updated_archive)
            conn.update(worksheet="Backlog", data=new_active_backlog)
            st.success("Jobs archived successfully.")
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.success("✅ Database is clean.")
