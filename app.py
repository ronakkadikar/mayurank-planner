import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection
import numpy as np

# --- CONFIGURATION ---
st.set_page_config(page_title="Mayurank ERP & Scheduler", layout="wide")
st.markdown("""
    <style>
    .stMetric { border: 1px solid #d3d3d3; padding: 15px; border-radius: 8px; background-color: #f9f9fa; }
    h1 { color: #1E3A8A; }
    .sugar { color: #d97706; }
    .dfs { color: #059669; }
    .cup { color: #4338ca; }
    .admin-box { border: 2px solid #dc2626; padding: 20px; border-radius: 10px; background-color: #fef2f2; }
    </style>
    """, unsafe_allow_html=True)

# --- CLOUD DB CONNECTION ---
conn = st.connection("gsheets", type=GSheetsConnection)
try:
    master_db = conn.read(worksheet="Backlog", ttl=0).dropna(subset=['Job_ID'])
except Exception as e:
    st.error(f"Database Connection Error: {e}")
    st.stop()

# --- MRP & HYBRID SCHEDULING LOGIC ---
def process_mrp_and_schedule(df):
    if df.empty: return df, pd.DataFrame()
    
    df['Required_KG'] = (df['Remaining_Qty'] * df['Pack_Size_Grams']) / 1000
    df['Missing_KG'] = df['Required_KG'] - df['Bulk_Stock_KG']
    df['Missing_KG'] = df['Missing_KG'].apply(lambda x: max(0, x))
    df['Status'] = np.where(df['Missing_KG'] > 0, 'BLOCKED', 'READY')
    
    blocked_pos = df[df['Status'] == 'BLOCKED']['PO_Number'].unique()
    
    ready_df = df[df['Status'] == 'READY'].copy()
    blocked_df = df[df['Status'] == 'BLOCKED'].copy()
    
    if ready_df.empty: return ready_df, blocked_df
    
    ready_df['Strict_PO_Delivery'] = ready_df['Strict_PO_Delivery'].astype(bool)
    ready_df['Dispatch_Status'] = ready_df.apply(
        lambda row: "⚠️ STAGE ONLY (PO Shortage)" if (row['Strict_PO_Delivery'] and row['PO_Number'] in blocked_pos) else "✅ Clear to Ship",
        axis=1
    )
    
    margin_weights = {'Very High': 5, 'High': 4, 'Medium': 3, 'Low': 2, 'Very Low': 1}
    ready_df['Margin_Score'] = ready_df['Margin_Class'].map(margin_weights).fillna(1)
    ready_df['Delivery_Date'] = pd.to_datetime(ready_df['Delivery_Date'], errors='coerce')
    
    strict_mask = ready_df['Strict_PO_Delivery'] == True
    strict_df = ready_df[strict_mask].copy()
    flexible_df = ready_df[~strict_mask].copy()
    
    if not strict_df.empty:
        po_overrides = strict_df.groupby('PO_Number').agg({
            'Is_Urgent': 'max',
            'Delivery_Date': 'min',
            'Client_Priority': 'min'
        }).to_dict()
        
        strict_df['Is_Urgent'] = strict_df['PO_Number'].map(po_overrides['Is_Urgent'])
        strict_df['Delivery_Date'] = strict_df['PO_Number'].map(po_overrides['Delivery_Date'])
        strict_df['Client_Priority'] = strict_df['PO_Number'].map(po_overrides['Client_Priority'])

    combined_df = pd.concat([strict_df, flexible_df], ignore_index=True)
    schedule_df = combined_df.sort_values(
        by=['Is_Urgent', 'Delivery_Date', 'Client_Priority', 'PO_Number', 'Margin_Score'],
        ascending=[False, True, True, True, False]
    ).reset_index(drop=True)
    
    schedule_df['Delivery_Date'] = schedule_df['Delivery_Date'].dt.strftime('%Y-%m-%d')
    return schedule_df, blocked_df

def assign_timing(df, hourly_cap):
    if df.empty: return df
    df['Est_Run_Time_Hrs'] = df['Remaining_Qty'] / hourly_cap
    df['Cumulative_Hrs'] = df['Est_Run_Time_Hrs'].cumsum()
    df['Shift_Window'] = df['Cumulative_Hrs'].apply(
        lambda x: "Standard (0-8)" if x <= 8 else ("Overtime (8-10)" if x <= 10 else "Pushed")
    )
    return df

# --- UI INTERFACE ---
st.title("🌐 Mayurank ERP: Scheduling & Procurement")

st.sidebar.header("⚙️ Dynamic Capacities")
dfs_cap = st.sidebar.number_input("DFS Line (Pkts/Hr)", value=3750, step=100)
sugar_cap = st.sidebar.number_input("Sugar Line (Pkts/Hr)", value=5000, step=500)
cup_cap = st.sidebar.number_input("Cup Filler [Pulse/Rice] (Pkts/Hr)", value=2500, step=100)

# Added 6th Tab for Admin Operations
t1, t2, t3, t4, t5, t6 = st.tabs(["📊 1. Dashboard", "📥 2. Upload Orders", "🗓️ 3. Production Lines", "🛒 4. Procurement", "✅ 5. End of Day", "🗄️ 6. System Admin"])

pending_df = master_db[master_db['Remaining_Qty'] > 0].copy()
ready_jobs, blocked_jobs = process_mrp_and_schedule(pending_df)

# --- TAB 1: EXECUTIVE DASHBOARD ---
with t1:
    st.subheader("📈 Active Roster Overview")
    if not master_db.empty:
        total_ordered = master_db['Ordered_Qty'].sum()
        total_remaining = master_db['Remaining_Qty'].sum()
        total_completed = total_ordered - total_remaining
        completion_pct = (total_completed / total_ordered) * 100 if total_ordered > 0 else 0
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Current Roster Orders", f"{total_ordered:,} pkts")
        m2.metric("Roster Completed", f"{total_completed:,} pkts")
        m3.metric("Current Backlog", f"{total_remaining:,} pkts")
        m4.metric("Roster Completion", f"{completion_pct:.1f}%")
        
        st.progress(completion_pct / 100)
        st.divider()
        
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            st.markdown("**Active Backlog by Category (Packets)**")
            if not pending_df.empty:
                cat_data = pending_df.groupby('Category')['Remaining_Qty'].sum()
                st.bar_chart(cat_data, color="#1E3A8A")
            else:
                st.info("No active backlog.")
                
        with col_chart2:
            st.markdown("**Procurement Readiness (Job Count)**")
            if not pending_df.empty:
                chart_df = pending_df.copy()
                chart_df['Req'] = (chart_df['Remaining_Qty'] * chart_df['Pack_Size_Grams']) / 1000
                chart_df['Status'] = np.where((chart_df['Req'] - chart_df['Bulk_Stock_KG']) > 0, 'Blocked (Missing Material)', 'Ready to Produce')
                status_counts = chart_df['Status'].value_counts()
                st.bar_chart(status_counts, color="#059669")
            else:
                st.info("No active jobs to analyze.")
    else:
        st.info("No data in the system yet. Please upload orders.")

# --- TAB 2: UPLOAD ---
with t2:
    st.subheader("Master Backlog Upload")
    uploaded_file = st.file_uploader("Upload Orders File (CSV/Excel)", type=['csv', 'xlsx'])
    if uploaded_file and st.button("Save to Cloud"):
        new_data = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('xlsx') else pd.read_csv(uploaded_file)
        new_data['Remaining_Qty'] = new_data['Ordered_Qty']
        updated_db = pd.concat([master_db, new_data], ignore_index=True).drop_duplicates(subset=['Job_ID'], keep='last')
        conn.update(worksheet="Backlog", data=updated_db)
        st.success("Orders saved! Go to Production Lines.")
        st.rerun()

# --- TAB 3: TRI-LINE SCHEDULE ---
with t3:
    if not ready_jobs.empty:
        sugar_df = assign_timing(ready_jobs[ready_jobs['Category'].str.lower() == 'sugar'].copy(), sugar_cap)
        cup_df = assign_timing(ready_jobs[ready_jobs['Category'].str.lower().isin(['pulse', 'rice'])].copy(), cup_cap)
        dfs_df = assign_timing(ready_jobs[~ready_jobs['Category'].str.lower().isin(['sugar', 'pulse', 'rice'])].copy(), dfs_cap)
        
        display_cols = ['PO_Number', 'Job_ID', 'SKU', 'Dispatch_Status', 'Remaining_Qty', 'Shift_Window', 'Delivery_Date']
        
        def highlight_staging(row):
            if "STAGE ONLY" in row['Dispatch_Status']: return ['background-color: #fff3cd'] * len(row)
            return [''] * len(row)
        
        st.markdown("<h3 class='dfs'>🚨 DFS Line (Spices/Dry Fruits)</h3>", unsafe_allow_html=True)
        if not dfs_df.empty: st.dataframe(dfs_df[display_cols].style.apply(highlight_staging, axis=1), use_container_width=True)
        else: st.info("No ready DFS jobs.")
        
        st.markdown("<h3 class='cup'>🥣 Cup Filler Line (Pulses/Rice)</h3>", unsafe_allow_html=True)
        if not cup_df.empty: st.dataframe(cup_df[display_cols].style.apply(highlight_staging, axis=1), use_container_width=True)
        else: st.info("No ready Cup Filler jobs.")
        
        st.markdown("<h3 class='sugar'>🍚 Dedicated Sugar Line</h3>", unsafe_allow_html=True)
        if not sugar_df.empty: st.dataframe(sugar_df[display_cols].style.apply(highlight_staging, axis=1), use_container_width=True)
        else: st.info("No ready Sugar jobs.")
    else:
        st.warning("No jobs are Clear-to-Build! Check Procurement tab.")

# --- TAB 4: PROCUREMENT ---
with t4:
    st.subheader("⚠️ Material Shortages (Blocked Jobs)")
    if not blocked_jobs.empty:
        st.error(f"WARNING: {len(blocked_jobs)} jobs cannot be scheduled due to raw material shortages.")
        st.markdown("#### Action Required for Purchasing:")
        buy_list = blocked_jobs.groupby('SKU')['Missing_KG'].sum().reset_index().sort_values(by='Missing_KG', ascending=False)
        st.dataframe(buy_list.style.format({'Missing_KG': "{:.1f} kg"}), use_container_width=True)
    else:
        st.success("All pending jobs have sufficient raw materials!")

# --- TAB 5: END OF DAY BULK OUTPUT ---
with t5:
    st.subheader("Bulk Record Actual Production")
    if not pending_df.empty:
        template_df = pending_df[['Job_ID', 'SKU', 'Category', 'Remaining_Qty']].copy()
        template_df['Actual_Produced'] = ""
        
        st.download_button(label="📥 Download Today's EOD Template", data=template_df.to_csv(index=False).encode('utf-8'), file_name="EOD_Template.csv", mime="text/csv")
        st.divider()
        
        eod_file = st.file_uploader("Upload Completed EOD Sheet (CSV/Excel)", type=['csv', 'xlsx'])
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
                        master_db.loc[master_db['Job_ID'].astype(str) == job_id, 'Remaining_Qty'] = max(0, expected - actual)
                        updated_count += 1
                
                conn.update(worksheet="Backlog", data=master_db)
                st.success(f"Successfully processed {updated_count} jobs! Cloud updated.")
                st.rerun()
    else:
        st.success("All jobs are completed!")

# --- TAB 6: SYSTEM ADMIN & MONTHLY ARCHIVE ---
with t6:
    st.subheader("🗄️ Database Hygiene & Maintenance")
    st.write("Over time, completed jobs will slow down the application. Use this routine at the end of the month to permanently move completed jobs (Remaining Qty = 0) out of the active database and into your historical Archive tab.")
    
    st.markdown("<div class='admin-box'>", unsafe_allow_html=True)
    completed_jobs = master_db[master_db['Remaining_Qty'] <= 0].copy()
    
    if not completed_jobs.empty:
        st.warning(f"**Action Required:** You have {len(completed_jobs)} fully completed jobs cluttering the active database.")
        if st.button("🚨 Execute Monthly Archive Routine"):
            try:
                # 1. Fetch existing Archive
                archive_db = conn.read(worksheet="Archive", ttl=0).dropna(subset=['Job_ID'])
                # 2. Append newly completed jobs
                updated_archive = pd.concat([archive_db, completed_jobs], ignore_index=True)
            except Exception:
                # If Archive tab is completely empty, just use the completed jobs
                updated_archive = completed_jobs
                
            # 3. Filter active backlog to keep ONLY pending jobs
            new_active_backlog = master_db[master_db['Remaining_Qty'] > 0]
            
            # 4. Push updates to both sheets simultaneously 
            conn.update(worksheet="Archive", data=updated_archive)
            conn.update(worksheet="Backlog", data=new_active_backlog)
            
            st.success(f"Success! {len(completed_jobs)} jobs have been permanently moved to the Archive sheet.")
            st.rerun()
    else:
        st.success("✅ Your database is clean! There are no fully completed jobs to archive right now.")
    st.markdown("</div>", unsafe_allow_html=True)
