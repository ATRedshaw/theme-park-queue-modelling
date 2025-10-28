# streamlit run dashboard/app.py
import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import time

st.set_page_config(layout="wide")

st.title('Theme Park Queue Time Analysis')

# --- Data Loading ---
@st.cache_data
def load_data():
    """Loads data from the SQLite database."""
    # Correct the path to be relative to the project root
    db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'queue_data.db')
    if not os.path.exists(db_path):
        st.error(f"Database not found at {db_path}")
        return None, None
    
    conn = sqlite3.connect(db_path)
    try:
        park_info = pd.read_sql_query("SELECT * FROM park_info", conn)
        queue_data = pd.read_sql_query("SELECT * FROM queue_data", conn)
    finally:
        conn.close()
    
    # --- Data Preprocessing ---
    queue_data['date'] = pd.to_datetime(queue_data['date'], format='%Y/%m/%d')
    queue_data['time_of_day'] = pd.to_datetime(queue_data['time_of_day'], format='%H:%M').dt.time
    queue_data['queue_time'] = pd.to_numeric(queue_data['queue_time'], errors='coerce')
    queue_data['is_closed'] = queue_data['is_closed'].astype(bool)
    queue_data.drop(columns=['id'], inplace=True)
    
    # Add day of week and month columns
    queue_data['day_of_week'] = queue_data['date'].dt.day_name()
    queue_data['month'] = queue_data['date'].dt.month_name()
    
    return park_info, queue_data

park_info, queue_data = load_data()

if park_info is None or queue_data is None:
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header('Filters')

# Park Selection
park_id = st.sidebar.selectbox(
    'Select Park',
    park_info['park_id'].unique()
)

# Filter data for the selected park
park_rides = park_info[park_info['park_id'] == park_id]
park_queue_data = queue_data[queue_data['ride_id'].isin(park_rides['ride_id'])]

# Ride Selection
selected_rides = st.sidebar.multiselect(
    'Select Rides',
    park_rides['ride_name'].unique(),
    default=park_rides['ride_name'].unique()
)
selected_ride_ids = park_rides[park_rides['ride_name'].isin(selected_rides)]['ride_id'].tolist()

# Date Range Selection
min_date = park_queue_data['date'].min().date()
max_date = park_queue_data['date'].max().date()

if 'selected_date_range' not in st.session_state:
    st.session_state.selected_date_range = [min_date, max_date]

def reset_date_range():
    st.session_state.selected_date_range = [min_date, max_date]

st.sidebar.date_input(
    'Select Date Range',
    key='selected_date_range',
    min_value=min_date,
    max_value=max_date,
)

st.sidebar.button('Reset Date Range', on_click=reset_date_range)

# Day of Week Selection
all_days = queue_data['day_of_week'].unique()
days_of_week = st.sidebar.multiselect(
    'Select Day of Week',
    all_days,
    default=all_days
)
if not days_of_week:
    days_of_week = all_days

# Month Selection
all_months = queue_data['month'].unique()
months = st.sidebar.multiselect(
    'Select Month',
    all_months,
    default=all_months
)
if not months:
    months = all_months

# Time of Day Selection
start_time = st.sidebar.time_input('Start time', time(0, 0))
end_time = st.sidebar.time_input('End time', time(23, 45))

# --- Filtering Logic ---
if len(st.session_state.selected_date_range) != 2:
    st.session_state.selected_date_range = [min_date, max_date]

filtered_data = park_queue_data[
    (park_queue_data['ride_id'].isin(selected_ride_ids)) &
    (park_queue_data['date'] >= pd.to_datetime(st.session_state.selected_date_range[0])) &
    (park_queue_data['date'] <= pd.to_datetime(st.session_state.selected_date_range[1])) &
    (park_queue_data['day_of_week'].isin(days_of_week)) &
    (park_queue_data['month'].isin(months)) &
    (park_queue_data['time_of_day'] >= start_time) &
    (park_queue_data['time_of_day'] <= end_time)
]

# --- Main Page ---
st.header(f'Queue Time Analysis for {park_id}')

if filtered_data.empty:
    st.warning("No data available for the selected filters.")
else:
    # Exclude closed rides and zero queue times for average calculation
    open_queue_data = filtered_data[
        (~filtered_data['is_closed']) & (filtered_data['queue_time'] > 0)
    ].copy()

    # Calculate average queue time
    average_queue_time = open_queue_data.groupby(['ride_id', 'time_of_day'])['queue_time'].mean().reset_index()
    average_queue_time.rename(columns={'queue_time': 'average_queue_time'}, inplace=True)
    
    # Merge with park info to get ride names
    average_queue_time = average_queue_time.merge(park_rides[['ride_id', 'ride_name']], on='ride_id', how='left')

    # Create a string representation of the time_of_day column for plotting
    average_queue_time['time_of_day_str'] = average_queue_time['time_of_day'].astype(str)

    # --- Plotting ---
    st.subheader('Average Queue Time per Ride')
    
    import plotly.express as px

    fig = px.line(
        average_queue_time,
        x='time_of_day_str',
        y='average_queue_time',
        color='ride_name',
        title='Average Queue Time Throughout the Day',
        labels={'time_of_day_str': 'Time of Day', 'average_queue_time': 'Average Queue Time (minutes)', 'ride_name': 'Ride'},
        markers=True
    )

    fig.update_layout(
        xaxis={'type': 'category'},
        legend_title_text='Rides'
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # --- Data Table ---
    st.subheader('Filtered Queue Data')
    # Merge with ride names for display
    display_data = filtered_data.merge(park_rides[['ride_id', 'ride_name']], on='ride_id', how='left')
    st.dataframe(display_data[['ride_name', 'date', 'time_of_day', 'queue_time', 'is_closed', 'day_of_week', 'month']].sort_values(by=['date', 'time_of_day', 'ride_name']))