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

# --- CLOUD DATABASE CONNECTION (The Loop & Memory) ---
# This connects securely to your Google Sheet
conn = st.connection("gsheets", type=GSheetsConnection)

# Fetch the current memory (ttl=0 ensures it doesn't use stale cached data)
try:
    master_db = conn.read(worksheet="Backlog", ttl=0)
    # Drop empty rows that Google Sheets sometimes adds
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

tab1, tab2, tab3 = st.tabs(["📥 1. Upload Orders", "🗓️ 2. Today's Plan", "✅ 3. End of Day Output"])

# --- TAB 1: UPLOAD (Write to Cloud) ---
with tab1:
    st.subheader("Add to Master Backlog")
    uploaded_file = st.file_uploader("Upload Orders File (CSV/Excel)", type=['csv', 'xlsx'])
    
    if uploaded_file and st.button("Process & Save to Cloud"):
        new_data = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('xlsx') else pd.read_csv(uploaded_file)
        new_data['Remaining_Qty'] = new_data['Ordered_Qty']
        
        # Combine old backlog with new orders
        updated_db = pd.concat([master_db, new_data], ignore_index=True)
        # Remove any duplicates by Job ID (keeping the newest)
        updated_db = updated_db.drop_duplicates(subset=['Job_ID'], keep='last')
        
        # Save back to Google Sheets
        conn.update(worksheet="Backlog", data=updated_db)
        st.success("Orders successfully saved to the Cloud Database!")
        st.rerun() # Refresh the app

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
            st.dataframe(dfs_schedule[display_cols].format({'Est_Run_Time_Hrs': "{:.2f}"}), use_container_width=True)
        
        st.divider()
        
        st.markdown("<h3 class='sugar-header'>🍚 Dedicated Sugar Line Schedule</h3>", unsafe_allow_html=True)
        sugar_schedule = generate_schedule(sugar_jobs, sugar_capacity)
        if not sugar_schedule.empty:
            st.dataframe(sugar_schedule[display_cols].format({'Est_Run_Time_Hrs': "{:.2f}"}), use_container_width=True)
    else:
        st.info("The backlog is empty.")

# --- TAB 3: END OF DAY OUTPUT (The Loop) ---
with tab3:
    st.subheader("Record Actual Production")
    active_jobs = master_db[master_db['Remaining_Qty'] > 0].copy()
    
    if not active_jobs.empty:
        with st.form("actuals_form"):
            active_jobs['Display_Name'] = active_jobs['Job_ID'].astype(str) + " - " + active_jobs['SKU']
            selected_display = st.selectbox("Select Job", active_jobs['Display_Name'].tolist())
            selected_job = selected_display.split(" - ")[0]
            
            expected_qty = active_jobs.loc[active_jobs['Job_ID'] == selected_job, 'Remaining_Qty'].values[0]
            actual_qty = st.number_input(f"Actual Packets Produced", min_value=0, value=int(expected_qty))
            
            if st.form_submit_button("Update Cloud Database"):
                new_remaining = max(0, expected_qty - actual_qty)
                # Update the specific row in the dataframe
                master_db.loc[master_db['Job_ID'] == selected_job, 'Remaining_Qty'] = new_remaining
                
                # Push the entire updated dataframe back to Google Sheets
                conn.update(worksheet="Backlog", data=master_db)
                st.success(f"Cloud updated! {selected_job} now has {new_remaining} remaining.")
                # We don't use st.rerun() inside a form callback, user can just click another tab
    else:
        st.success("All jobs are completed!")
