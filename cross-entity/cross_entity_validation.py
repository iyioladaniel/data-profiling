import pandas as pd
from typing import Dict, List
import logging

def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def analyze_bvn_duplicates(file_paths: Dict[str, str], columns_mapping: Dict[str, Dict[str, str]]) -> tuple:
    """
    Analyze BVN duplicates across multiple datasets.
    
    Args:
        file_paths: Dictionary mapping dataset names to file paths
        columns_mapping: Dictionary mapping dataset names to their column mappings
        
    Returns:
        tuple: (DataFrame with BVN analysis, DataFrame with missing BVNs)
    """
    logging.info("Starting BVN duplicate analysis")
    
    bvn_records: List[pd.DataFrame] = []
    missing_bvn_records: List[pd.DataFrame] = []
    
    # Process each dataset
    for dataset_name, file_path in file_paths.items():
        try:
            logging.info(f"Processing dataset: {dataset_name}")
            
            # Read dataset with only required columns
            df = pd.read_csv(
                file_path,
                usecols=[
                    columns_mapping[dataset_name]["id"],
                    columns_mapping[dataset_name]["bvn"]
                ]
            )
            
            # Rename columns for consistency
            df.rename(columns={
                columns_mapping[dataset_name]["id"]: "id",
                columns_mapping[dataset_name]["bvn"]: "BVN"
            }, inplace=True)
            
            # Add dataset identifier
            df["dataset"] = dataset_name
            
            # Handle missing BVNs
            missing_mask = df["BVN"].isna()
            if missing_mask.any():
                missing_records = df[missing_mask].copy()
                missing_records["reason"] = "Missing BVN"
                missing_bvn_records.append(missing_records)
                logging.warning(f"Found {missing_mask.sum()} missing BVNs in {dataset_name}")
            
            # Remove rows with missing BVNs for main analysis
            df = df[~missing_mask]
            
            bvn_records.append(df)
            
        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
            continue
        except Exception as e:
            logging.error(f"Error processing {dataset_name}: {str(e)}")
            continue
    
    if not bvn_records:
        raise ValueError("No valid data found in any dataset")
    
    # Combine all valid records
    bvn_df = pd.concat(bvn_records, ignore_index=True)
    
    # Process duplicates
    bvn_df["duplicated?"] = bvn_df["BVN"].duplicated(keep=False)
    
    # Find first occurrence of duplicated IDs
    duplicate_mapping = bvn_df[bvn_df["duplicated?"]].groupby("BVN")["id"].first().to_dict()
    bvn_df["duplicated_id"] = bvn_df["BVN"].map(lambda x: duplicate_mapping.get(x, ""))
    
    # Create missing BVNs DataFrame if any were found
    missing_bvn_df = pd.concat(missing_bvn_records, ignore_index=True) if missing_bvn_records else None
    
    return bvn_df, missing_bvn_df

def main():
    """Main execution function."""
    setup_logging()
    
    # Configuration
    file_paths = {
        "asset-management": "./asset_custdata.csv",
        "trustees-digital": "./digital_trustees_custdata.csv",
        "trustees-traditional": "./traditional_trustees_custdata.csv",
        #"insurance": "./individual_insurance_custdata.csv",
        "registrars": "./registrars_shold.csv",
        "securities" : "./securities_custdata.csv"
    }
    
    columns_mapping = {
        "asset-management": {"id": "CustAID", "bvn": "CustomerBVN"},
        "trustees-digital": {"id": "platformuserid", "bvn": "Bvn"},
        "trustees-traditional": {"id": "CustAID", "bvn": "CustomerBVN"},
        #"insurance": {"id": "Customer ID", "bvn": "userBVN"},
        "registrars": {"id": "Acctno", "bvn": "bvn"},
        "securities": {"id": "CustAID", "bvn": "CustomerBVN"}
    }
    
    try:
        # Perform analysis
        bvn_df, missing_bvn_df = analyze_bvn_duplicates(file_paths, columns_mapping)
        
        # Save results
        bvn_df.to_csv("bvn_comparison.csv", index=False)
        logging.info("BVN comparison saved to 'bvn_comparison.csv'")
        
        if missing_bvn_df is not None:
            missing_bvn_df.to_csv("missing_bvns.csv", index=False)
            logging.info("Missing BVNs saved to 'missing_bvns.csv'")
        
    except Exception as e:
        logging.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()