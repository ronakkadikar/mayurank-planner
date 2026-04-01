import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection

# --- CONFIGURATION & SETUP ---
st.set_page_config(page_title="Mayurank Master Planner", layout="wide")

st.markdown("""
    <style>
    .stMetric { border: 1px solid #d3d3d3; padding: 15px; border-radius: 8px; background-color: #f9f9fa; }
    h1 { color: #1E3A8A; }
    .sugar-header { color: #d97706; }
    .dfs-header { color: #059669; }
    </style>
    """, unsafe_allow_html=True)

# --- CLOUD DATABASE CONNECTION ---
conn = st.connection("gsheets", type=GSheetsConnection)

try:
    master_db = conn.read(worksheet="Backlog", ttl=0)
    master_db = master_db.dropna(subset=['Job_ID']) 
except Exception as e:
    st.error(f"Could not connect to Google Sheets. Error: {e}")
    st.stop()

# --- CORE SCHEDULING LOGIC ---
def generate_schedule(df, hourly_cap):
    if df.empty: return df
    margin_weights = {'High': 3, 'Medium': 2, 'Low': 1}
    df['Margin_Score'] = df['Margin_Class'].map(margin_weights).fillna(1)
    df = df.sort_values(by=['Is_Urgent', 'Margin_Score', 'Client_Weight'], ascending=[False, False, False]).reset_index(drop=True)
    df['Est_Run_Time_Hrs'] = df['Remaining_Qty'] / hourly_cap
    df['Cumulative_Hrs'] = df['Est_Run_Time_Hrs'].cumsum()
    
    def assign_shift(cum_hrs):
        if cum_hrs <= 8: return "Standard Shift (0-8 Hrs)"
        elif cum_hrs <= 10: return "Overtime (8-10 Hrs)"
        else: return "Pushed to Tomorrow"
            
    df['Production_Window'] = df['Cumulative_Hrs'].apply(assign_shift)
    return df

# --- USER INTERFACE ---
st.title("☁️ Mayurank Cloud Operations Hub")

st.sidebar.header("⚙️ Daily Machine Capacities")
dfs_capacity = st.sidebar.number_input("DFS Line Capacity (Pkts/Hr)", value=3750, step=100)
sugar_capacity = st.sidebar.number_input("Sugar Machine Capacity (Pkts/Hr)", value=5000, step=500)

tab1, tab2, tab3 = st.tabs(["📥 1. Upload Orders", "🗓️ 2. Today's Plan", "✅ 3. End of Day Bulk Output"])

# --- TAB 1: UPLOAD ---
with tab1:
    st.subheader("Add to Master Backlog")
    uploaded_file = st.file_uploader("Upload Orders File (CSV/Excel)", type=['csv', 'xlsx'], key="new_orders")
    
    if uploaded_file and st.button("Process & Save to Cloud"):
        new_data = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('xlsx') else pd.read_csv(uploaded_file)
        new_data['Remaining_Qty'] = new_data['Ordered_Qty']
        
        updated_db = pd.concat([master_db, new_data], ignore_index=True)
        updated_db = updated_db.drop_duplicates(subset=['Job_ID'], keep='last')
        
        conn.update(worksheet="Backlog", data=updated_db)
        st.success("Orders successfully saved to the Cloud Database!")
        st.rerun()

# --- TAB 2: ROUTING & PLANNING ---
with tab2:
    pending_df = master_db[master_db['Remaining_Qty'] > 0].copy()
    
    if not pending_df.empty:
        pending_df['Is_Urgent'] = pending_df['Is_Urgent'].astype(bool)
        sugar_jobs = pending_df[pending_df['Category'].str.lower() == 'sugar'].copy()
        dfs_jobs = pending_df[pending_df['Category'].str.lower() != 'sugar'].copy()
        
        st.markdown("<h3 class='dfs-header'>🚨 DFS Line Schedule (Bottleneck)</h3>", unsafe_allow_html=True)
        dfs_schedule = generate_schedule(dfs_jobs, dfs_capacity)
        if not dfs_schedule.empty:
            display_cols = ['Job_ID', 'SKU', 'Category', 'Remaining_Qty', 'Est_Run_Time_Hrs', 'Production_Window']
            st.dataframe(dfs_schedule[display_cols].style.format({'Est_Run_Time_Hrs': "{:.2f}"}), use_container_width=True)
        else:
            st.success("DFS Line has no pending jobs!")
        
        st.divider()
        
        st.markdown("<h3 class='sugar-header'>🍚 Dedicated Sugar Line Schedule</h3>", unsafe_allow_html=True)
        sugar_schedule = generate_schedule(sugar_jobs, sugar_capacity)
        if not sugar_schedule.empty:
            st.dataframe(sugar_schedule[display_cols].style.format({'Est_Run_Time_Hrs': "{:.2f}"}), use_container_width=True)
        else:
            st.info("No Sugar jobs in the backlog.")
    else:
        st.info("The backlog is empty.")

# --- TAB 3: END OF DAY BULK OUTPUT ---
with tab3:
    st.subheader("Bulk Record Actual Production")
    st.write("Download the template, fill in the actual quantities produced today (leave skipped items blank), and upload it back.")
    
    active_jobs = master_db[master_db['Remaining_Qty'] > 0].copy()
    
    if not active_jobs.empty:
        # 1. Provide the Downloadable Template
        template_df = active_jobs[['Job_ID', 'SKU', 'Category', 'Remaining_Qty']].copy()
        template_df['Actual_Produced'] = "" # Create an empty column for user input
        
        st.download_button(
            label="📥 Download Today's EOD Template", 
            data=template_df.to_csv(index=False).encode('utf-8'), 
            file_name="EOD_Template.csv", 
            mime="text/csv"
        )
        
        st.divider()
        
        # 2. Upload the Completed Template
        eod_file = st.file_uploader("Upload Completed EOD Sheet (CSV/Excel)", type=['csv', 'xlsx'], key="eod_upload")
        
        if eod_file and st.button("Process Bulk EOD Data"):
            eod_data = pd.read_csv(eod_file) if eod_file.name.endswith('.csv') else pd.read_excel(eod_file)
            
            # Data Validation
            if 'Job_ID' not in eod_data.columns or 'Actual_Produced' not in eod_data.columns:
                st.error("Upload failed: The file must contain exactly the 'Job_ID' and 'Actual_Produced' columns from the template.")
            else:
                # Process the bulk update
                updated_count = 0
                for index, row in eod_data.iterrows():
                    job_id = str(row['Job_ID']).strip()
                    
                    # Treat blanks, NaN, or text as 0
                    actual = pd.to_numeric(row['Actual_Produced'], errors='coerce')
                    if pd.isna(actual):
                        actual = 0
                        
                    # Find the job in the master database and update it
                    if job_id in master_db['Job_ID'].astype(str).values:
                        expected = master_db.loc[master_db['Job_ID'].astype(str) == job_id, 'Remaining_Qty'].values[0]
                        new_remaining = max(0, expected - actual)
                        master_db.loc[master_db['Job_ID'].astype(str) == job_id, 'Remaining_Qty'] = new_remaining
                        updated_count += 1
                
                # Push the updated master database back to Google Sheets
                conn.update(worksheet="Backlog", data=master_db)
                st.success(f"Successfully processed {updated_count} jobs! The Cloud Database has been updated.")
                
    else:
        st.success("All jobs are completed! No EOD processing required.")
