import pandas as pd

am = pd.read_csv('./asset_csv/Assetmanagement_CustData_Sheet1.csv')
securities = pd.read_csv('./securities_csv/Cosec_PersonalDetails.csv')
trustees_d =  pd.read_csv('./trustees_csv/digitaltrusteesusersandcustomers_Sheet1.csv')
trustees_t =  pd.read_csv('./trustees_csv/TraditionalTrusteescustomerdata_Sheet1.csv')

entities = [am, securities, trustees_d, trustees_t]
names = ['Asset_Management', 'Securities', 'Digital_Trustees', 'Traditional_Trustees']

def analyze_bvn_duplicates(dfs: list, names: list = None):
    """
    Analyze duplicate BVNs across multiple dataframes.

    Parameters:
    - dfs: List of pandas DataFrames
    - names: Optional list of names to identify each dataframe in output

    Returns:
    - A dictionary with dataframe name as key and grouped analysis as value
    """
    results = {}
    
    for i, df in enumerate(dfs):
        # Get a readable name
        df_name = names[i] if names and i < len(names) else f"df_{i+1}"

        # Identify customer ID and BVN columns
        cust_col = next((col for col in df.columns if 'cust' in col.lower()), None)
        bvn_col = next((col for col in df.columns if 'bvn' in col.lower()), None)

        if not cust_col or not bvn_col:
            print(f"Skipping {df_name}: required columns not found.")
            continue

        # Find duplicate BVNs
        bvn_duplicates = df[df.duplicated(subset=bvn_col, keep=False)]

        # Group by BVN and customer ID
        grouped = bvn_duplicates.groupby([bvn_col, cust_col]).size().reset_index(name="count")
        
        results[df_name] = grouped

    return results

if __name__ == "__main__":
    # Example usage
    analysis = analyze_bvn_duplicates(entities, names)

    # Print the results
    for name, df in analysis.items():
        print(f"Analysis for {name}:\n", df)
        df.to_csv(f'{name}_bvn_analysis.csv', index=False)
        print(f"Individual analysis for {name} saved to {name}_bvn_analysis.csv")
    
    # Save the analysis to a CSV file
    #analysis_df = pd.concat(analysis.values(), keys=analysis.keys())
    #analysis_df.to_csv('bvn_analysis.csv', index=False)
    #print("Analysis saved to bvn_analysis.csv")
    