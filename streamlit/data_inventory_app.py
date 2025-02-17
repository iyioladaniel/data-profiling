import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Data Inventory Explorer",
    page_icon="📊",
    layout="wide"
)

# Custom CSS
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
    /* Tooltip styles that work in both themes */
    .tooltip-content {
        visibility: hidden;
        background-color: var(--background-color, #ffffff);
        color: var(--text-color, #000000);
        border: 1px solid var(--border-color, #ddd);
        padding: 8px;
        border-radius: 4px;
        position: absolute;
        z-index: 1000;
        width: 200px;
    }
    
    .tooltip:hover .tooltip-content {
        visibility: visible;
    }
    /* Updated section-info for dark theme compatibility */
    .section-info {
        background-color: rgba(255, 255, 255, 0.1);
        padding: 1rem;
        border-radius: 5px;
        margin-bottom: 1rem;
        border: 1px solid rgba(255, 255, 255, 0.2);
    }
    /* Dark theme specific adjustments */
    [data-theme="dark"] .section-info {
        background-color: rgba(255, 255, 255, 0.1);
    }
    [data-theme="light"] .section-info {
        background-color: #f8f9fa;
    }
    .network-graph {
        border: 1px solid #ddd;
        border-radius: 5px;
        padding: 10px;
    }
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
    }
    </style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data(file_path):
    return pd.read_csv(file_path)

def render_section_info(title, description):
    """Renders a section header with informative description."""
    st.markdown(f"""
    <div class="section-info">
        <h3>{title}</h3>
        <p>{description}</p>
    </div>
    """, unsafe_allow_html=True)

def render_header():
    col1, col2 = st.columns([1, 5])
    with col1:
        st.image("./KPMG.png", width=100)
    with col2:
        st.title("📊 Data Inventory Explorer")
        st.markdown("""
        Explore and analyze your organization's data inventory with advanced filtering and visualization capabilities.
        Use the sidebar filters to narrow down your analysis and explore relationships between different data elements.
        """)

def render_sidebar_filters(df):
    with st.sidebar:
        st.header("🔍 Filters")
        
        if st.button("Reset All Filters", use_container_width=True):
            return {"entity": [], "domain": [], "table": [], "field": "", 
                   "data_type": [], "nullable": "", "has_relationships": None}
        
        st.divider()
        
        filters = {
            "entity": st.multiselect("Entity", options=sorted(df['Entity'].unique())),
            "domain": st.multiselect("Domain", options=sorted(df['Domain'].unique())),
            "table": st.multiselect("Table Name", options=sorted(df['Table Name'].unique())),
            "field": st.text_input("Field Name"),
            "data_type": st.multiselect("Data Type", options=sorted(df['Data Type'].unique())),
            "nullable": st.selectbox("Nullable", options=["", "Yes", "No"]),
            "has_relationships": st.radio(
                "Relationship Status",
                options=[None, "Has Relationships", "No Relationships"]
            )
        }
        
        return filters

@st.cache_data
def filter_dataframe(df, filters):
    mask = pd.Series(True, index=df.index)
    
    if filters["entity"]:
        mask &= df['Entity'].isin(filters["entity"])
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
    if filters["has_relationships"] == "Has Relationships":
        mask &= df['Relationship Mapping'].notna()
    elif filters["has_relationships"] == "No Relationships":
        mask &= df['Relationship Mapping'].isna()
    
    return df[mask]

def create_enhanced_network_graph(df):
    G = nx.Graph()
    node_info = {}  # Store comprehensive node information
    edges = []
    
    # Remove the problematic line since we don't actually use field_mapping
    # field_mapping = df.set_index('Field Name').to_dict('index')
    
    for _, row in df.iterrows():
        if pd.notna(row['Relationship Mapping']):
            # Source node (current field)
            source_field = row['Field Name']
            source_id = f"{row['Entity']}.{row['Table Name']}.{source_field}"
            
            # Store comprehensive node information
            node_info[source_id] = {
                'type': 'field',
                'field': source_field,
                'table': row['Table Name'],
                'entity': row['Entity'],
                'domain': row['Domain'],
                'data_type': row['Data Type'],
                'nullable': row['Nullable (Yes/No)']
            }
            
            # Add table node
            table_id = f"{row['Entity']}.{row['Table Name']}"
            node_info[table_id] = {
                'type': 'table',
                'table': row['Table Name'],
                'entity': row['Entity'],
                'domain': row['Domain']
            }
            
            # Add edge between field and its table
            edges.append((source_id, table_id))
            
            # Process relationships
            related_fields = str(row['Relationship Mapping']).split(',')
            for target in related_fields:
                target = target.strip()
                # Try to find the full context for the target field
                target_info = df[df['Field Name'] == target].iloc[0] if not df[df['Field Name'] == target].empty else None
                
                if target_info is not None:
                    target_id = f"{target_info['Entity']}.{target_info['Table Name']}.{target}"
                    node_info[target_id] = {
                        'type': 'field',
                        'field': target,
                        'table': target_info['Table Name'],
                        'entity': target_info['Entity'],
                        'domain': target_info['Domain'],
                        'data_type': target_info['Data Type'],
                        'nullable': target_info['Nullable (Yes/No)']
                    }
                    edges.append((source_id, target_id))
    
    # Add nodes and edges to graph
    G.add_nodes_from(node_info.keys())
    G.add_edges_from(edges)
    
    return G, node_info

def render_enhanced_network_visualization(G, node_info, filtered_df):
    render_section_info(
        "Relationship Network Visualization",
        """Interactive visualization showing relationships between tables and fields. 
        Hover over nodes to see detailed information. Tables are shown as squares, fields as circles. 
        Colors represent different domains. Use filters to focus on specific areas."""
    )
    
    if G.number_of_edges() > 0:
        # Create figure with larger size and better spacing
        plt.figure(figsize=(20, 12))
        
        # Use a more spaced out layout
        pos = nx.spring_layout(G, k=2, iterations=50)
        
        # Create color scheme for domains
        domains = list(set(info['domain'] for info in node_info.values()))
        color_map = plt.cm.get_cmap('tab20')(np.linspace(0, 1, len(domains)))
        domain_colors = dict(zip(domains, color_map))
        
        # Draw nodes by type
        node_types = {'table': [], 'field': []}
        node_colors = {'table': [], 'field': []}
        node_labels = {}
        
        for node, info in node_info.items():
            node_types[info['type']].append(node)
            node_colors[info['type']].append(domain_colors[info['domain']])
            
            # Create more informative labels
            if info['type'] == 'table':
                node_labels[node] = f"{info['entity']}\n{info['table']}"
            else:
                node_labels[node] = info['field']
        
        # Draw table nodes (squares)
        nx.draw_networkx_nodes(G, pos,
                             nodelist=node_types['table'],
                             node_color=node_colors['table'],
                             node_size=3000,
                             node_shape='s',
                             alpha=0.7)
        
        # Draw field nodes (circles)
        nx.draw_networkx_nodes(G, pos,
                             nodelist=node_types['field'],
                             node_color=node_colors['field'],
                             node_size=1000,
                             node_shape='o',
                             alpha=0.7)
        
        # Draw edges with slight transparency
        nx.draw_networkx_edges(G, pos,
                             edge_color='gray',
                             alpha=0.5,
                             width=1)
        
        # Add labels with better formatting
        nx.draw_networkx_labels(G, pos,
                              labels=node_labels,
                              font_size=8,
                              font_weight='bold')
        
        # Add legend for domains
        legend_elements = [plt.Line2D([0], [0],
                                    marker='o',
                                    color='w',
                                    markerfacecolor=color,
                                    label=domain,
                                    markersize=10)
                         for domain, color in domain_colors.items()]
        
        plt.legend(handles=legend_elements,
                  title='Domains',
                  loc='center left',
                  bbox_to_anchor=(1, 0.5))
        
        plt.title("Data Inventory Relationship Network")
        
        # Add node information as tooltip
        tooltip_text = ""
        for node, info in node_info.items():
            if info['type'] == 'field':
                tooltip_text += f"""
                Field: {info['field']}
                Table: {info['table']}
                Entity: {info['entity']}
                Domain: {info['domain']}
                Data Type: {info['data_type']}
                Nullable: {info['nullable']}
                ---
                """
        
        # Display the graph
        st.pyplot(plt.gcf())
        
        # Add network metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Nodes", G.number_of_nodes())
        with col2:
            st.metric("Total Connections", G.number_of_edges())
        with col3:
            st.metric("Tables", len(node_types['table']))
        with col4:
            st.metric("Fields", len(node_types['field']))
            
        # Add searchable table of relationships
        st.subheader("Relationship Details")
        relationships_data = []
        for node, info in node_info.items():
            if info['type'] == 'field':
                neighbors = list(G.neighbors(node))
                for neighbor in neighbors:
                    if node_info[neighbor]['type'] == 'field':
                        relationships_data.append({
                            'Source Entity': info['entity'],
                            'Source Table': info['table'],
                            'Source Field': info['field'],
                            'Target Entity': node_info[neighbor]['entity'],
                            'Target Table': node_info[neighbor]['table'],
                            'Target Field': node_info[neighbor]['field']
                        })
        
        if relationships_data:
            relationships_df = pd.DataFrame(relationships_data)
            st.dataframe(relationships_df, use_container_width=True)
    else:
        st.info("No relationships found in the filtered dataset.")

def render_visualizations(df):
    render_section_info(
        "Data Distribution Analysis",
        """Explore the distribution of data across different dimensions using interactive visualizations.
        The donut charts show the breakdown by data type and domain, while the relationship analysis
        shows the proportion of fields with defined relationships."""
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        data_type_counts = df['Data Type'].value_counts()
        fig1 = go.Figure(data=[go.Pie(
            labels=data_type_counts.index,
            values=data_type_counts.values,
            hole=0.4,
            title="Data Type Distribution"
        )])
        fig1.update_layout(showlegend=True)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        domain_counts = df['Domain'].value_counts()
        fig2 = go.Figure(data=[go.Pie(
            labels=domain_counts.index,
            values=domain_counts.values,
            hole=0.4,
            title="Distribution by Domain"
        )])
        st.plotly_chart(fig2, use_container_width=True)
    
    relationship_stats = df['Relationship Mapping'].notna().value_counts()
    fig3 = go.Figure(data=[go.Pie(
        labels=['With Relations', 'No Relations'],
        values=relationship_stats.values,
        hole=0.4,
        title="Fields with Relationships",
        marker_colors=['#2ecc71', '#e74c3c']
    )])
    st.plotly_chart(fig3, use_container_width=True)

def render_cross_company_analysis(df):
    render_section_info(
        "Cross-Entity Analysis",
        """Compare data structures and relationships across different companies and domains. 
        This analysis helps identify patterns in data organization and potential areas for standardization."""
    )
    
    st.subheader("Table Structure Comparison")
    structure_analysis = df.groupby(['Entity', 'Domain']).agg({
        'Table Name': 'nunique',
        'Field Name': 'count',
        'Data Type': lambda x: x.nunique(),
        'Relationship Mapping': lambda x: x.notna().sum()
    }).round(2)
    
    structure_analysis.columns = [
        'Unique Tables',
        'Total Fields',
        'Distinct Data Types',
        'Related Fields'
    ]
    
    st.dataframe(structure_analysis, use_container_width=True)
    
    st.subheader("Data Type Distribution by Entity")
    datatype_dist = pd.crosstab(df['Entity'], df['Data Type'])
    fig = px.bar(datatype_dist, 
                 title="Data Type Usage Across Companies",
                 labels={'value': 'Count', 'variable': 'Data Type'},
                 barmode='group')
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Relationship Density Analysis")
    relationship_density = df.groupby('Entity').agg({
        'Field Name': 'count',
        'Relationship Mapping': lambda x: x.notna().sum()
    })
    relationship_density['Relationship Density'] = (
        relationship_density['Relationship Mapping'] / 
        relationship_density['Field Name'] * 100
    ).round(2)
    
    fig = px.bar(relationship_density,
                 y='Relationship Density',
                 title="Relationship Density by Entity (%)",
                 labels={'Relationship Density': 'Percentage of Fields with Relationships'})
    st.plotly_chart(fig, use_container_width=True)

def main():
    # Load data
    df = load_data("dummy_data_inventory.csv")
    
    # Render UI components
    render_header()
    filters = render_sidebar_filters(df)
    filtered_df = filter_dataframe(df, filters)
    
    # Main content
    render_section_info(
        "Data Inventory Table",
        """Browse the complete data inventory below. Use the filters in the sidebar to narrow down the results.
        You can sort columns by clicking on the headers and export the filtered data as CSV."""
    )
    
    st.dataframe(filtered_df, use_container_width=True, height=400)
    
    if not filtered_df.empty:
        if st.button("Export Filtered Data", use_container_width=True):
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="filtered_data_inventory.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Tables", filtered_df['Table Name'].nunique())
        with col2:
            st.metric("Total Fields", len(filtered_df))
        with col3:
            st.metric("Unique Data Types", filtered_df['Data Type'].nunique())
        with col4:
            st.metric("Fields with Relations", filtered_df['Relationship Mapping'].notna().sum())
        
        # Visualizations
        render_visualizations(filtered_df)
        
        # Network visualization
        G, node_info = create_enhanced_network_graph(filtered_df)
        render_enhanced_network_visualization(G, node_info, filtered_df)
        
        # Cross-entity analysis
        render_cross_company_analysis(filtered_df)
    else:
        st.warning("No data matches the selected filters. Please adjust your selection.")

if __name__ == "__main__":
    main()