import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Data Inventory Explorer",
    page_icon="📊",
    layout="wide"  # This makes the app use the full width of the browser
)

# Custom CSS to improve UI
st.markdown("""
    <style>
    .main > div {
        padding-top: 2rem;
    }
    .stDataFrame {
        width: 100%;
    }
    .row-widget.stButton > button {
        width: 100%;
    }
    .css-1d391kg {
        padding-top: 1rem;
    }
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
    }
    </style>
""", unsafe_allow_html=True)

@st.cache_data  # Cache the data loading
def load_data(file_path):
    return pd.read_csv(file_path)

# Header section with improved layout
def render_header():
    col1, col2 = st.columns([1, 5])
    with col1:
        st.image("./KPMG.png", width=100)
    with col2:
        st.title("📊 Data Inventory Explorer")
        st.markdown("""
        Explore and analyze your organization's data inventory with advanced filtering and visualization capabilities.
        """)

# Sidebar filters with improved organization
def render_sidebar_filters(df):
    with st.sidebar:
        st.header("🔍 Filters")
        
        # Add reset filters button at the top
        if st.button("Reset All Filters", use_container_width=True):
            return {"company": "", "domain": "", "table": "", "field": "", 
                   "data_type": "", "nullable": ""}
        
        st.divider()
        
        # Create multiselect filters instead of text inputs for better UX
        filters = {
            "company": st.multiselect("Company", options=sorted(df['Company'].unique())),
            "domain": st.multiselect("Domain", options=sorted(df['Domain'].unique())),
            "table": st.multiselect("Table Name", options=sorted(df['Table Name'].unique())),
            "field": st.text_input("Field Name"),  # Keep as text input due to potentially large number of fields
            "data_type": st.multiselect("Data Type", options=sorted(df['Data Type'].unique())),
            "nullable": st.selectbox("Nullable", options=["", "Yes", "No"])
        }
        
        return filters

@st.cache_data
def filter_dataframe(df, filters):
    mask = pd.Series(True, index=df.index)
    
    if filters["company"]:
        mask &= df['Company'].isin(filters["company"])
    if filters["domain"]:
        mask &= df['Domain'].isin(filters["domain"])
    if filters["table"]:
        mask &= df['Table Name'].isin(filters["table"])
    if filters["field"]:
        mask &= df['Field Name'].str.contains(filters["field"], case=False, na=False)
    if filters["data_type"]:
        mask &= df['Data Type'].isin(filters["data_type"])
    if filters["nullable"]:
        mask &= df['Nullable (Yes/No)'] == filters["nullable"]
    
    return df[mask]

def render_main_content(df, filtered_df):
    # Data table with improved width
    st.header("📋 Data Inventory")
    st.dataframe(
        filtered_df,
        use_container_width=True,
        height=400  # Fixed height for better layout
    )
    
    # Export functionality
    if st.button("Export Filtered Data", use_container_width=True):
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="filtered_data_inventory.csv",
            mime="text/csv",
            use_container_width=True
        )

def render_statistics(df, filtered_df):
    st.header("📊 Summary Statistics")
    
    # Key metrics in a 4-column layout
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Tables", filtered_df['Table Name'].nunique())
    with col2:
        st.metric("Total Fields", len(filtered_df))
    with col3:
        st.metric("Data Types", filtered_df['Data Type'].nunique())
    with col4:
        st.metric("Nullable Fields", (filtered_df['Nullable (Yes/No)'] == 'Yes').sum())

def render_visualizations(filtered_df):
    st.header("📈 Data Analysis")
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Data Type Distribution
        data_type_counts = filtered_df['Data Type'].value_counts()
        fig1 = px.bar(
            data_type_counts,
            title="Data Type Distribution",
            labels={'index': 'Data Type', 'value': 'Count'},
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig1.update_layout(showlegend=False)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Domain Distribution
        domain_counts = filtered_df['Domain'].value_counts()
        fig2 = px.pie(
            values=domain_counts.values,
            names=domain_counts.index,
            title="Distribution by Domain",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    # Relationship Analysis
    if 'Relationship Mapping' in filtered_df.columns:
        st.subheader("🔗 Field Relationships")
        relationship_stats = filtered_df['Relationship Mapping'].notna().value_counts()
        fig3 = px.pie(
            values=relationship_stats.values,
            names=['With Relations', 'No Relations'],
            title="Fields with Relationships",
            color_discrete_sequence=['#2ecc71', '#e74c3c']
        )
        st.plotly_chart(fig3, use_container_width=True)

def main():
    # Load data
    df = load_data("data_inventory.csv")
    
    # Render UI components
    render_header()
    filters = render_sidebar_filters(df)
    filtered_df = filter_dataframe(df, filters)
    
    # Main content
    render_main_content(df, filtered_df)
    
    # Statistics and visualizations
    if not filtered_df.empty:
        render_statistics(df, filtered_df)
        render_visualizations(filtered_df)
    else:
        st.warning("No data matches the selected filters. Please adjust your selection.")

if __name__ == "__main__":
    main()