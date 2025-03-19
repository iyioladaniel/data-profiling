import pandas as pd

def pivot_identification_column(filepath):
    """Pivot Identification type and Identification Number fo"""
    df = pd.read_csv(filepath)
    # Pivot the data
    df_transformed = df.pivot_table(index="platformuserid", columns="IdentificationType", values="IdentificationNumber", aggfunc="first")
    # Reset index to turn multi-index into columns
    df_transformed.reset_index(inplace=True)
    # Rename columns for clarity (optional)
    df_transformed.columns.name = None  # Remove multi-index name
    df_transformed.iloc[:,1:] = df_transformed.iloc[:,1:].apply(pd.to_numeric, errors='coerce').astype('Int64')
    
    merged_details = pd.merge(
            df, df_transformed,
            on='platformuserid',
            how='right'
        ).sort_values(['platformuserid'])
    
    merged_details.to_csv("digital_trustees_custdata_pivot.csv", index=False)
    
if __name__ == "__main__":
    # Call the validation function
    pivot_identification_column("./dummy-data/digital_trustees_custdata.csv")