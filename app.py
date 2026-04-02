import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection
import numpy as np
from datetime import datetime

# --- CONFIGURATION ---
st.set_page_config(page_title="Mayurank ERP & Scheduler", layout="wide")
st.markdown("""
    <style>
    .stMetric { border: 1px solid #d3d3d3; padding: 15px; border-radius: 8px; background-color: #ffffff; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    h1 { color: #1E3A8A; }
    .sugar { color: #d97706; }
    .dfs { color: #059669; }
    .cup { color: #4338ca; }
    .late-metric { color: #dc2626; font-weight: bold; }
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
    
    # 1. MRP (Material Check)
    df['Required_KG'] = (df['Remaining_Qty'] * df['Pack_Size_Grams']) / 1000
    df['Missing_KG'] = df['Required_KG'] - df['Bulk_Stock_KG']
    df['Missing_KG'] = df['Missing_KG'].apply(lambda x: max(0, x))
    df['Status'] = np.where(df['Missing_KG'] > 0, 'BLOCKED', 'READY')
    
    ready_df = df[df['Status'] == 'READY'].copy()
    blocked_df = df[df['Status'] == 'BLOCKED'].copy()
    
    if ready_df.empty: return ready_df, blocked_df
    
    # 2. IDENTIFY EXACT MISSING SKUS FOR STAGING
    # Group the blocked items by PO and combine their SKU names into a string
    missing_skus_dict = blocked_df.groupby('PO_Number')['SKU'].apply(lambda x: ', '.join(x)).to_dict()
    
    ready_df['Strict_PO_Delivery'] = ready_df['Strict_PO_Delivery'].astype(bool)
    
    # Inject the missing SKUs into the Dispatch Status
    def get_dispatch_status(row):
        if row['Strict_PO_Delivery'] and row['PO_Number'] in missing_skus_dict:
            return f"⚠️ STAGE ONLY (Missing: {missing_skus_dict[row['PO_Number']]})"
        return "✅ Clear to Ship"
        
    ready_df['Dispatch_Status'] = ready_df.apply(get_dispatch_status, axis=1)
    
    # 3. HYBRID SCHEDULING LOGIC
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
    if df.empty or hourly_cap == 0: return df
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

t1, t2, t3, t4, t5, t6 = st.tabs(["📊 1. Exec Dashboard", "📥 2. Upload Orders", "🗓️ 3. Production Lines", "🛒 4. Procurement", "✅ 5. End of Day", "🗄️ 6. System Admin"])

pending_df = master_db[master_db['Remaining_Qty'] > 0].copy()
ready_jobs, blocked_jobs = process_mrp_and_schedule(pending_df)

# --- TAB 1: EXECUTIVE DASHBOARD (OVERHAULED) ---
with t1:
    st.subheader("📈 Operations Command Center")
    if not pending_df.empty:
        # 1. CORE HEALTH METRICS
        today = pd.Timestamp.now().normalize()
        pending_df['Delivery_Date'] = pd.to_datetime(pending_df['Delivery_Date'], errors='coerce')
        
        total_pending_pkts = pending_df['Remaining_Qty'].sum()
        late_jobs = pending_df[pending_df['Delivery_Date'] <= today]
        late_count = len(late_jobs)
        blocked_count = len(blocked_jobs)
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Active Jobs in Queue", f"{len(pending_df)}")
        c2.metric("Total Pending Packets", f"{total_pending_pkts:,}")
        c3.metric("🚨 Jobs Past Due / Due Today", f"{late_count}", help="Needs immediate Overtime or Expediting")
        c4.metric("⚠️ Material Blocked Jobs", f"{blocked_count}", help="Waiting on Procurement")
        
        st.divider()
        
        # 2. BOTTLENECK LOAD ANALYSIS (Hours of Work)
        st.markdown("### ⚙️ Machine Load (Backlog in Hours)")
        st.write("Indicates how many hours of continuous running are required to clear the current queue.")
        
        sugar_qty = pending_df[pending_df['Category'].str.lower() == 'sugar']['Remaining_Qty'].sum()
        cup_qty = pending_df[pending_df['Category'].str.lower().isin(['pulse', 'rice'])]['Remaining_Qty'].sum()
        dfs_qty = pending_df[~pending_df['Category'].str.lower().isin(['sugar', 'pulse', 'rice'])]['Remaining_Qty'].sum()
        
        load_dfs = dfs_qty / dfs_cap if dfs_cap > 0 else 0
        load_cup = cup_qty / cup_cap if cup_cap > 0 else 0
        load_sugar = sugar_qty / sugar_cap if sugar_cap > 0 else 0
        
        lc1, lc2, lc3 = st.columns(3)
        lc1.metric("DFS Line Load", f"{load_dfs:.1f} Hrs", delta=f"{load_dfs/8:.1f} Shifts" if load_dfs > 0 else "0", delta_color="inverse")
        lc2.metric("Cup Filler Load", f"{load_cup:.1f} Hrs", delta=f"{load_cup/8:.1f} Shifts" if load_cup > 0 else "0", delta_color="inverse")
        lc3.metric("Sugar Line Load", f"{load_sugar:.1f} Hrs", delta=f"{load_sugar/8:.1f} Shifts" if load_sugar > 0 else "0", delta_color="normal")

        st.divider()

        # 3. PROFITABILITY & RISK CHARTS
        ch1, ch2 = st.columns(2)
        with ch1:
            st.markdown("**Backlog by Profit Margin (Packets)**")
            margin_dist = pending_df.groupby('Margin_Class')['Remaining_Qty'].sum()
            st.bar_chart(margin_dist, color="#059669")
        with ch2:
            st.markdown("**Order Status Breakdown**")
            # Calculate what is Ready, Staged, and Blocked
            status_summary = {"Clear to Produce": len(ready_jobs[~ready_jobs['Dispatch_Status'].str.contains("STAGE ONLY")])}
            status_summary["Produce & Stage"] = len(ready_jobs[ready_jobs['Dispatch_Status'].str.contains("STAGE ONLY")])
            status_summary["Blocked (Missing Material)"] = len(blocked_jobs)
            st.bar_chart(pd.Series(status_summary), color="#1E3A8A")

    else:
        st.info("The factory floor is entirely clean! No pending orders.")

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
    st.subheader("🗄️ Database Hygiene")
    completed_jobs = master_db[master_db['Remaining_Qty'] <= 0].copy()
    if not completed_jobs.empty:
        st.warning(f"**Action Required:** You have {len(completed_jobs)} fully completed jobs to archive.")
        if st.button("🚨 Execute Archive Routine"):
            try:
                archive_db = conn.read(worksheet="Archive", ttl=0).dropna(subset=['Job_ID'])
                updated_archive = pd.concat([archive_db, completed_jobs], ignore_index=True)
            except Exception:
                updated_archive = completed_jobs
                
            new_active_backlog = master_db[master_db['Remaining_Qty'] > 0]
            conn.update(worksheet="Archive", data=updated_archive)
            conn.update(worksheet="Backlog", data=new_active_backlog)
            st.success(f"Success! {len(completed_jobs)} jobs archived.")
            st.rerun()
    else:
        st.success("✅ Database is clean.")
