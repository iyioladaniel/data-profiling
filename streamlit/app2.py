import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Data Inventory Explorer",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for better UI
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
    .network-graph {
        border: 1px solid #ddd;
        border-radius: 5px;
        padding: 10px;
    }
    </style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data(file_path):
    df = pd.read_csv(file_path)
    # Basic data validation
    df['Data Quality Score'] = calculate_data_quality(df)
    return df

def calculate_data_quality(df):
    # Calculate a simple data quality score based on completeness
    quality_score = df.notna().mean(axis=1) * 100
    return quality_score.round(2)

def render_header():
    col1, col2 = st.columns([1, 5])
    with col1:
        st.image("./KPMG.png", width=100)
    with col2:
        st.title("📊 Data Inventory Explorer")
        st.markdown("""
        Explore and analyze your organization's data inventory with advanced filtering and visualization capabilities.
        """)

def render_advanced_filters(df):
    with st.sidebar:
        st.header("🔍 Advanced Filters")
        
        if st.button("Reset All Filters", use_container_width=True):
            return {"company": "", "domain": "", "table": "", "field": "", 
                   "data_type": "", "nullable": "", "quality_threshold": 0,
                   "has_relationships": None, "field_length": None}
        
        st.divider()
        
        # Basic filters
        filters = {
            "company": st.multiselect("Company", options=sorted(df['Company'].unique())),
            "domain": st.multiselect("Domain", options=sorted(df['Domain'].unique())),
            "table": st.multiselect("Table Name", options=sorted(df['Table Name'].unique())),
            "field": st.text_input("Field Name"),
            "data_type": st.multiselect("Data Type", options=sorted(df['Data Type'].unique())),
            "nullable": st.selectbox("Nullable", options=["", "Yes", "No"])
        }
        
        # Advanced filtering options
        st.subheader("Advanced Options")
        filters["quality_threshold"] = st.slider(
            "Minimum Data Quality Score (%)", 
            0, 100, 0
        )
        
        filters["has_relationships"] = st.radio(
            "Relationship Status",
            options=[None, "Has Relationships", "No Relationships"]
        )
        
        filters["field_length"] = st.slider(
            "Field Length Range",
            0, df['Field Length'].max() if 'Field Length' in df.columns else 100,
            (0, df['Field Length'].max() if 'Field Length' in df.columns else 100)
        )
        
        return filters

@st.cache_data
def filter_dataframe(df, filters):
    mask = pd.Series(True, index=df.index)
    
    # Basic filters
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
    
    # Advanced filters
    if filters["quality_threshold"] > 0:
        mask &= df['Data Quality Score'] >= filters["quality_threshold"]
    
    if filters["has_relationships"] == "Has Relationships":
        mask &= df['Relationship Mapping'].notna()
    elif filters["has_relationships"] == "No Relationships":
        mask &= df['Relationship Mapping'].isna()
    
    if 'Field Length' in df.columns:
        mask &= (df['Field Length'] >= filters["field_length"][0]) & \
                (df['Field Length'] <= filters["field_length"][1])
    
    return df[mask]

def create_network_graph(df):
    G = nx.Graph()
    edges = []
    
    for _, row in df.iterrows():
        if pd.notna(row['Relationship Mapping']):
            source = f"{row['Table Name']}.{row['Field Name']}"
            G.add_node(source, type='source')
            
            related_fields = str(row['Relationship Mapping']).split(',')
            for target in related_fields:
                target = target.strip()
                G.add_node(target, type='target')
                edges.append((source, target))
    
    G.add_edges_from(edges)
    
    return G

def render_network_visualization(G):
    st.subheader("🔗 Relationship Network Graph")
    
    if G.number_of_edges() > 0:
        fig, ax = plt.subplots(figsize=(15, 10))
        pos = nx.spring_layout(G, k=1, iterations=50)
        
        # Draw nodes
        nx.draw_networkx_nodes(G, pos, 
                             node_color='lightblue',
                             node_size=1000,
                             alpha=0.7)
        
        # Draw edges
        nx.draw_networkx_edges(G, pos, 
                             edge_color='gray',
                             alpha=0.5)
        
        # Draw labels
        nx.draw_networkx_labels(G, pos, 
                              font_size=8,
                              font_weight='bold')
        
        plt.title("Field Relationships Network")
        st.pyplot(fig)
    else:
        st.info("No relationships found in the filtered dataset.")

def render_summary_stats(df):
    st.header("📊 Cross-Company Domain Analysis")
    
    # Create summary statistics
    summary = df.groupby(['Company', 'Domain']).agg({
        'Table Name': 'nunique',
        'Field Name': 'count',
        'Data Type': 'nunique',
        'Nullable (Yes/No)': lambda x: (x == 'Yes').sum(),
        'Relationship Mapping': lambda x: x.notna().sum(),
        'Data Quality Score': 'mean'
    }).round(2)
    
    summary.columns = [
        'Unique Tables',
        'Total Fields',
        'Unique Data Types',
        'Nullable Fields',
        'Fields with Relations',
        'Avg Quality Score'
    ]
    
    st.dataframe(summary, use_container_width=True)
    
    # Create heatmap
    fig = px.density_heatmap(
        df,
        x='Company',
        y='Domain',
        z='Data Quality Score',
        title='Data Quality Score by Company and Domain',
        color_continuous_scale='Viridis'
    )
    st.plotly_chart(fig, use_container_width=True)

def render_visualizations(filtered_df):
    st.header("📈 Data Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Data Type Distribution as donut chart
        data_type_counts = filtered_df['Data Type'].value_counts()
        fig1 = go.Figure(data=[go.Pie(
            labels=data_type_counts.index,
            values=data_type_counts.values,
            hole=0.4,
            title="Data Type Distribution"
        )])
        fig1.update_layout(showlegend=True)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Domain Distribution as donut chart
        domain_counts = filtered_df['Domain'].value_counts()
        fig2 = go.Figure(data=[go.Pie(
            labels=domain_counts.index,
            values=domain_counts.values,
            hole=0.4,
            title="Distribution by Domain"
        )])
        st.plotly_chart(fig2, use_container_width=True)
    
    # Relationship Analysis
    if 'Relationship Mapping' in filtered_df.columns:
        relationship_stats = filtered_df['Relationship Mapping'].notna().value_counts()
        fig3 = go.Figure(data=[go.Pie(
            labels=['With Relations', 'No Relations'],
            values=relationship_stats.values,
            hole=0.4,
            title="Fields with Relationships",
            marker_colors=['#2ecc71', '#e74c3c']
        )])
        st.plotly_chart(fig3, use_container_width=True)

def render_relationship_analysis(df):
    st.header("🔍 Detailed Relationship Analysis")
    
    # Calculate relationship metrics
    total_fields = len(df)
    fields_with_relations = df['Relationship Mapping'].notna().sum()
    relation_percentage = (fields_with_relations / total_fields * 100).round(2)
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Fields with Relations", fields_with_relations)
    with col2:
        st.metric("Total Fields", total_fields)
    with col3:
        st.metric("Relation Coverage", f"{relation_percentage}%")
    
    # Relationship patterns
    if fields_with_relations > 0:
        st.subheader("Common Relationship Patterns")
        relationship_patterns = df[df['Relationship Mapping'].notna()].groupby(
            ['Table Name', 'Field Name', 'Relationship Mapping']
        ).size().reset_index(name='count')
        st.dataframe(relationship_patterns.sort_values('count', ascending=False))

def main():
    # Load data
    df = load_data("data_inventory.csv")
    
    # Render UI components
    render_header()
    filters = render_advanced_filters(df)
    filtered_df = filter_dataframe(df, filters)
    
    # Main content
    st.header("📋 Data Inventory")
    st.dataframe(filtered_df, use_container_width=True, height=400)
    
    if not filtered_df.empty:
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
        
        # Visualizations
        render_visualizations(filtered_df)
        
        # Network graph
        G = create_network_graph(filtered_df)
        render_network_visualization(G)
        
        # Summary statistics
        render_summary_stats(filtered_df)
        
        # Detailed relationship analysis
        render_relationship_analysis(filtered_df)
    else:
        st.warning("No data matches the selected filters. Please adjust your selection.")

if __name__ == "__main__":
    main()