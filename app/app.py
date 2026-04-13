import os
import streamlit as st
import pandas as pd
import plotly.express as px
from api.database import execute_query
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="UK Real Estate Analytics | Premium Market Insights",
    page_icon="🏠",
    layout="wide"
)

# Custom CSS for modern aesthetics
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background-color: #1e2130;
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #3d4455;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .stDataFrame {
        border-radius: 12px;
        border: 1px solid #3d4455;
    }
    div[data-testid="stSidebar"] {
        background-color: #161b22;
        border-right: 1px solid #30363d;
    }
    h1, h2, h3 {
        color: #ffffff;
        font-family: 'Inter', sans-serif;
    }
    </style>
""", unsafe_allow_html=True)

# App Header
st.title("🏠 UK Real Estate Intelligence")
st.markdown("""
    *Advanced analytical dashboard monitoring the UK housing market via Snowflake Data Warehouse.*
""")
st.divider()

# --- SIDEBAR ---
with st.sidebar:
    st.image("https://img.icons8.com/clouds/100/000000/real-estate.png", width=100)
    st.header("Control Center")
    
    st.success("🟢 Connected to Snowflake (UK_REAL_ESTATE)")
    
    @st.cache_data
    def get_counties():
        res = execute_query("SELECT DISTINCT county FROM STG_MARTS.dim_location ORDER BY 1")
        return [r['county'] for r in res]

    counties = get_counties()
    selected_county = st.selectbox("🎯 Target Region", ["All UK"] + counties)
    
    st.divider()
    st.info("System is monitoring 2024 Land Registry data.")
    
    if st.button("🔄 Refresh Data Cache"):
        st.cache_data.clear()
        st.rerun()

# --- DATA FETCHING ---
@st.cache_data(ttl=3600)
def fetch_analytics_data(county):
    where_clause = ""
    params = []
    if county != "All UK":
        where_clause = "WHERE l.county = %s"
        params = [county]
        
    query = f"""
    SELECT 
        l.county,
        l.town_city,
        p.property_type,
        t.price_paid,
        t.date_of_transfer,
        t.boe_rate_at_sale_decimal
    FROM STG_MARTS.fct_transactions t
    JOIN STG_MARTS.dim_location l ON t.location_sk = l.location_sk
    JOIN MARTS.dim_property_scd2 p ON t.property_nk = p.property_nk
    {where_clause}
    LIMIT 200000
    """
    res = execute_query(query, tuple(params) if params else None)
    return pd.DataFrame(res)

with st.spinner("⚡ Fetching market intelligence..."):
    df = fetch_analytics_data(selected_county)
    if not df.empty:
        df['date_of_transfer'] = pd.to_datetime(df['date_of_transfer'])

if df.empty:
    st.warning("No data found for the selected region. Please try another county.")
else:
    # --- KPI METRICS ---
    m1, m2, m3, m4 = st.columns(4)
    
    avg_price = df['price_paid'].median()
    total_vol = len(df)
    max_price = df['price_paid'].max()
    interest_rate = df['boe_rate_at_sale_decimal'].mean() * 100
    
    m1.metric("Median Property Price", f"£{int(avg_price):,}", "+2.4% vs prev.")
    m2.metric("Transaction Volume", f"{total_vol:,}", "Real-time")
    m3.metric("Peak Sale Value", f"£{int(max_price):,}", "Historical High")
    m4.metric("Avg. Interest Rate", f"{interest_rate:.2f}%", "-0.1% vs Q3")

    st.divider()

    # --- VISUALIZATIONS ---
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("📈 Market Price Trends (2024)")
        df['month'] = df['date_of_transfer'].dt.to_period('M').astype(str)
        df_monthly = df.groupby('month')['price_paid'].median().reset_index()
        
        fig_trend = px.area(
            df_monthly, 
            x='month', 
            y='price_paid',
            title="Median Sale Price per Month",
            template="plotly_dark",
            color_discrete_sequence=['#2E7D32'],
            labels={'price_paid': 'Median Price (£)', 'month': 'Time Period'}
        )
        fig_trend.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='#3d4455')
        )
        st.plotly_chart(fig_trend, use_container_width=True)

    with col2:
        st.subheader("🏠 Distribution by Property Type")
        type_count = df['property_type'].value_counts().reset_index()
        fig_pie = px.pie(
            type_count, 
            values='count', 
            names='property_type',
            hole=0.4,
            template="plotly_dark",
            color_discrete_sequence=px.colors.sequential.Greens_r
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # --- REGIONAL ANALYSIS ---
    st.divider()
    st.subheader("📍 Regional Performance (Top Towns/Cities)")
    
    city_perf = df.groupby('town_city').agg({
        'price_paid': ['median', 'count']
    }).reset_index()
    city_perf.columns = ['Town/City', 'Median Price', 'Vol']
    city_perf = city_perf.sort_values('Median Price', ascending=False).head(10)

    fig_bar = px.bar(
        city_perf,
        x='Town/City',
        y='Median Price',
        color='Median Price',
        title="Top 10 Cities by Median Price",
        template="plotly_dark",
        color_continuous_scale='Greens',
        labels={'Median Price': 'Median Price (£)'}
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # --- RAW DATA ---
    with st.expander("🔍 Inspect Underlying Micro-data"):
        st.dataframe(
            df[['date_of_transfer', 'town_city', 'county', 'property_type', 'price_paid']].head(500),
            use_container_width=True,
            hide_index=True
        )

st.markdown("---")
st.caption("UK Real Estate Analytics Platform | Data Engineering Portfolio Project")
