import streamlit as st
import pandas as pd
import plotly.express as px
from api.database import execute_query

# Page config sets the look and feel
st.set_page_config(
    page_title="UK Real Estate Analytics 🇬🇧",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS to make it look premium
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background-color: #1e2130;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #3d4455;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🏠 UK Real Estate Market Intelligence")
st.markdown("---")

# 1. Sidebar for filters
st.sidebar.header("🔍 Filters")

@st.cache_data
def get_all_counties():
    res = execute_query("SELECT DISTINCT county FROM STG_MARTS.dim_location ORDER BY 1")
    return [r['county'] for r in res]

counties = get_all_counties()
selected_county = st.sidebar.selectbox("Select County", ["All"] + counties)

@st.cache_data
def get_dashboard_data(county):
    where_clause = ""
    params = []
    if county != "All":
        where_clause = "WHERE l.county = %s"
        params = [county]
        
    query = f"""
    SELECT 
        l.county,
        p.property_type,
        t.price_paid,
        t.date_of_transfer,
        t.boe_rate_at_sale_decimal
    FROM STG_MARTS.fct_transactions t
    JOIN STG_MARTS.dim_location l ON t.location_sk = l.location_sk
    JOIN MARTS.dim_property_scd2 p ON t.property_nk = p.property_nk
    {where_clause}
    LIMIT 100000
    """
    res = execute_query(query, tuple(params) if params else None)
    return pd.DataFrame(res)

with st.spinner("⚡ Connecting to Snowflake..."):
    df = get_dashboard_data(selected_county)

if df.empty:
    st.warning("No data found for the selected filters.")
else:
    # 2. Top Level KPIs
    col1, col2, col3, col4 = st.columns(4)
    avg_price = df['price_paid'].median()
    total_vol = len(df)
    max_price = df['price_paid'].max()
    
    col1.metric("Median Price", f"£{int(avg_price):,}")
    col2.metric("Total Transactions", f"{total_vol:,}")
    col3.metric("Highest Sale", f"£{int(max_price):,}")
    col4.metric("Market Sentiment", "Bullish" if avg_price > 400000 else "Stable")

    st.markdown("### 📈 Market Trends & Analysis")
    
    c1, c2 = st.columns(2)
    
    # 3. Chart: Price Over Time
    with c1:
        st.subheader("Price Trend (2024)")
        df['date_of_transfer'] = pd.to_datetime(df['date_of_transfer'])
        df_monthly = df.resample('ME', on='date_of_transfer')['price_paid'].median().reset_index()
        fig_line = px.line(df_monthly, x='date_of_transfer', y='price_paid', 
                          title="Median Price by Month", 
                          template="plotly_dark",
                          labels={'price_paid': 'Price (£)', 'date_of_transfer': 'Month'})
        st.plotly_chart(fig_line, use_container_width=True)

    # 4. Chart: Distribution by Property Type
    with c2:
        st.subheader("Volume by Property Type")
        type_vol = df['property_type'].value_counts().reset_index()
        fig_pie = px.pie(type_vol, values='count', names='property_type', 
                        hole=.3, template="plotly_dark")
        st.plotly_chart(fig_pie, use_container_width=True)

    # 5. Data View
    with st.expander("📄 View Raw Transactions"):
        st.dataframe(df.head(100), use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.info("This dashboard is connected live to a Snowflake Data Warehouse using a Medallion Architecture.")
