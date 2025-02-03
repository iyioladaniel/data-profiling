import streamlit as st
import pandas as pd
import plotly.express as px
import networkx as nx
import matplotlib.pyplot as plt

# Load data
def load_data(file):
    return pd.read_csv(file)

# Branding Section
st.set_page_config(
    page_title="Data Inventory Explorer",  # Sets the browser tab title
    layout="wide",
    page_icon="📊"  # Optional: Adds an emoji as the favicon
)

# Display the company logo
st.image("./KPMG.png", width=200)  # Replace "company_logo.png" with your logo file path

# App title and introduction
st.title("📊 Data Inventory Explorer")
st.markdown("""
**Welcome to the Data Inventory Explorer!**

This app allows you to navigate, filter, and visualize the data inventory for your organization. Use the filters in the sidebar to explore tables, fields, and relationships. For more details, expand the rows in the table below or view the relationship mapping graph.
""")

# File handling
uploaded_file = st.file_uploader("Upload your Data Inventory CSV (optional)", type=["csv"])
if uploaded_file:
    df = load_data(uploaded_file)
else:
    df = load_data("data_inventory.csv")  # Default file path

# Sidebar filters
with st.sidebar:
    st.header("🔍 Filter Data")
    company = st.text_input("Company")
    domain = st.text_input("Domain")
    table_name = st.text_input("Table Name")
    field_name = st.text_input("Field Name")
    data_type = st.text_input("Data Type")
    nullable = st.selectbox("Nullable", ["", "Yes", "No"])

# Filter data
def filter_data(df, company, domain, table_name, field_name, data_type, nullable):
    if company:
        df = df[df['Company'].str.contains(company, case=False, na=False)]
    if domain:
        df = df[df['Domain'].str.contains(domain, case=False, na=False)]
    if table_name:
        df = df[df['Table Name'].str.contains(table_name, case=False, na=False)]
    if field_name:
        df = df[df['Field Name'].str.contains(field_name, case=False, na=False)]
    if data_type:
        df = df[df['Data Type'].str.contains(data_type, case=False, na=False)]
    if nullable:
        df = df[df['Nullable (Yes/No)'].str.contains(nullable, case=False, na=False)]
    return df

filtered_df = filter_data(df, company, domain, table_name, field_name, data_type, nullable)

# Display data table
st.subheader("📋 Data Inventory Table")
st.dataframe(filtered_df)

# Export filtered data
if st.button("Export Filtered Data"):
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="filtered_data.csv",
        mime="text/csv"
    )

# Summary statistics
st.subheader("📊 Summary Statistics")
col1, col2 = st.columns(2)
with col1:
    st.metric("Total Tables", df['Table Name'].nunique())
    st.metric("Total Fields", df['Field Name'].nunique())
with col2:
    st.metric("Unique Data Types", df['Data Type'].nunique())

# Data Type Distribution
st.subheader("🛠️ Data Type Distribution")
# fig = px.bar(df['Data Type'].value_counts().reset_index(), x='index', y='Data Type', title="Data Type Counts")
# st.plotly_chart(fig)
data_type_counts = df['Data Type'].value_counts()
fig = px.bar(x=data_type_counts.index, y=data_type_counts.values, title="Data Type Counts")
st.plotly_chart(fig)

# Relationship Mapping
st.subheader("🔗 Relationship Mapping")
def visualize_relationships(df):
    G = nx.Graph()
    for _, row in df.iterrows():
        if pd.notna(row['Relationship Mapping']):
            related_fields = row['Relationship Mapping'].split(',')
            for related in related_fields:
                G.add_edge(row['Field Name'], related.strip())
    
    plt.figure(figsize=(10, 6))
    nx.draw(G, with_labels=True, node_color='lightblue', edge_color='gray', node_size=2000, font_size=10)
    st.pyplot(plt)

visualize_relationships(df)

# Expandable rows for details
st.subheader("🔍 Detailed View")
for index, row in filtered_df.iterrows():
    with st.expander(f"Details for {row['Field Name']}"):
        st.write(row)