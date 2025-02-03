import streamlit as st
import pandas as pd
import plotly.express as px
import networkx as nx
import matplotlib.pyplot as plt

# Load data
def load_data(file):
    return pd.read_csv(file)

# Branding Section
col1, col2 = st.columns([1, 3])  # Adjust the ratio as needed

with col1:
    st.image("./KPMG.png", width=75)  # Replace with your logo file path

with col2:
    st.title("📊 Data Inventory Explorer")
    st.markdown("""
    **Welcome to the Data Inventory Explorer!**

    This app allows you to navigate, filter, and visualize the data inventory for your organization. Use the filters in the sidebar to explore tables, fields, and relationships.
    """)

# File handling
#uploaded_file = st.file_uploader("Upload your Data Inventory CSV (optional)", type=["csv"])
# if uploaded_file:
#     df = load_data(uploaded_file)
# else:
#     df = load_data("data_inventory.csv")  # Default file path
df = load_data("data_inventory.csv")


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
st.dataframe(filtered_df, use_container_width=True)

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
data_type_counts = df['Data Type'].value_counts().reset_index()
data_type_counts.columns = ['Data Type', 'Count']
fig = px.bar(data_type_counts, x='Data Type', y='Count', title="Data Type Counts")
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

# Summary Statistics by Company and Domain
st.subheader("📊 Summary Statistics by Company and Domain")
company_domain_stats = (
    df.groupby(["Company", "Domain"])
    .agg(
        Total_Tables=("Table Name", "nunique"),
        Total_Fields=("Field Name", "count"),
        Unique_Data_Types=("Data Type", "nunique"),
        Nullable_Fields=("Nullable (Yes/No)", lambda x: (x == "Yes").sum()),
        Non_Nullable_Fields=("Nullable (Yes/No)", lambda x: (x == "No").sum()),
        Fields_with_Relationships=("Relationship Mapping", lambda x: (~x.isna()).sum())
    )
    .reset_index()
)
st.dataframe(company_domain_stats, use_container_width=True)

# Visualizations
st.subheader("📊 Total Tables per Company and Domain")
fig_tables = px.bar(
    company_domain_stats,
    x="Company",
    y="Total_Tables",
    color="Domain",
    title="Total Tables by Company and Domain",
    barmode="group"
)
st.plotly_chart(fig_tables, use_container_width=True)

st.subheader("📊 Field Distribution Across Companies")
fig_fields = px.pie(
    company_domain_stats,
    names="Company",
    values="Total_Fields",
    title="Field Distribution Across Companies"
)
st.plotly_chart(fig_fields, use_container_width=True)

st.subheader("📊 Nullable vs Non-Nullable Fields by Company")
nullable_stats = company_domain_stats.melt(
    id_vars=["Company", "Domain"],
    value_vars=["Nullable_Fields", "Non_Nullable_Fields"],
    var_name="Field_Type",
    value_name="Count"
)
fig_nullable = px.bar(
    nullable_stats,
    x="Company",
    y="Count",
    color="Field_Type",
    title="Nullable vs Non-Nullable Fields by Company",
    barmode="stack"
)
st.plotly_chart(fig_nullable, use_container_width=True)

st.subheader("📊 Unique Data Types per Company and Domain")
fig_heatmap = px.density_heatmap(
    company_domain_stats,
    x="Company",
    y="Domain",
    z="Unique_Data_Types",
    title="Unique Data Types by Company and Domain",
    color_continuous_scale="Viridis"
)
st.plotly_chart(fig_heatmap, use_container_width=True)


# Detailed View with Pagination
# st.subheader("🔍 Detailed View")
# rows_per_page = 10  # Number of rows per page
# total_rows = len(filtered_df)
# num_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0)

# page_number = st.number_input("Page Number", min_value=1, max_value=num_pages, value=1)
# start_idx = (page_number - 1) * rows_per_page
# end_idx = start_idx + rows_per_page

# paginated_df = filtered_df.iloc[start_idx:end_idx]

# for index, row in paginated_df.iterrows():
#     with st.expander(f"Details for {row['Field Name']}"):
#         st.write(row)