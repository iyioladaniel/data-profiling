import pandas as pd
from typing import Dict, List
import logging
import sys
from itertools import combinations

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
            missing_mask = (df["BVN"].isna()) | (df["BVN"]=="-")
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
    
    # Create serial id column
    bvn_df["serial_no"] = bvn_df.index + 1
    
    # Process duplicates
    bvn_df["duplicated?"] = bvn_df["BVN"].duplicated(keep=False)
    
    # Find first occurrence of duplicated IDs
    duplicate_mapping = bvn_df[bvn_df["duplicated?"]].groupby("BVN")["serial_no"].first().to_dict()
    bvn_df["duplicated_serial_no"] = bvn_df["BVN"].map(lambda x: duplicate_mapping.get(x, ""))
    
    # Create missing BVNs DataFrame if any were found
    missing_bvn_df = pd.concat(missing_bvn_records, ignore_index=True) if missing_bvn_records else None
    
    return bvn_df, missing_bvn_df

def load_data(file_path):
    """
    Load and validate input data
    Returns DataFrame and exits on error
    """
    try:
        df = pd.read_csv(file_path)
        
        # Validate required columns
        required_columns = ['customer_id', 'BVN', 'entity', 'serial_no', 'duplicated?', 'duplicated_serial_no']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logging.error(f"Missing required columns: {', '.join(missing)}")
            sys.exit(1)
            
        # Convert BVN to string to handle potential leading zeros
        df['BVN'] = df['BVN'].astype(str)
        
        return df
    
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        sys.exit(1)

def generate_reports(df):
    """
    Generate analytical reports
    Returns tuple of (unique_counts, cross_entity, merged_details, entity_combinations)
    """
    try:
        # Unique BVNs per entity
        unique_counts = df.groupby('entity')['BVN'].nunique().reset_index()
        unique_counts.columns = ['Entity', 'Unique BVN Count']
        
        # Cross-entity BVNs
        cross_entity = df.groupby('BVN').agg(
            entity_count=('entity', 'nunique'),
            entities=('entity', lambda x: ', '.join(sorted(x.unique())))
        ).reset_index()
        
        # Generate all possible entity combinations and their counts
        unique_entities = sorted(df['entity'].unique())
        entity_combinations = []
        
        # For each possible number of entities (2 through total number of entities)
        for i in range(2, len(unique_entities) + 1):
            # Generate all possible combinations of that size
            for combo in combinations(unique_entities, i):
                # Find BVNs that appear in all entities in this combination
                mask = cross_entity['entities'].apply(
                    lambda x: all(entity in x.split(', ') for entity in combo)
                )
                bvns_in_combo = cross_entity[mask]['BVN'].tolist()
                
                if bvns_in_combo:  # Only add if there are matching BVNs
                    entity_combinations.append({
                        'Combination Size': i,
                        'Entities': ' & '.join(combo),
                        'BVN Count': len(bvns_in_combo),
                        'BVNs': ', '.join(bvns_in_combo)
                    })
        
        # Create DataFrame and sort it
        entity_combinations_df = pd.DataFrame(entity_combinations)
        if not entity_combinations_df.empty:
            entity_combinations_df = entity_combinations_df.sort_values(
                ['Combination Size', 'BVN Count'], 
                ascending=[True, False]
            )
        
        # Detailed records with duplicate information
        merged_details = pd.merge(
            df[['BVN', 'entity', 'customer_id', 'serial_no', 'duplicated?', 'duplicated_serial_no']],
            cross_entity[['BVN', 'entity_count', 'entities']],
            on='BVN',
            how='right'
        ).sort_values(['BVN', 'entity'])
        
        return unique_counts, cross_entity, merged_details, entity_combinations_df
        
    except Exception as e:
        logging.error(f"Error generating reports: {str(e)}")
        raise  # Re-raise the exception to see the full error message

def save_excel_report(results, filename='bvn_analysis_report.xlsx'):
    """Save all results to Excel with multiple sheets"""
    try:
        unique_counts, cross_entity, merged_details, entity_combinations = results
        
        with pd.ExcelWriter(filename) as writer:
            unique_counts.to_excel(writer, sheet_name='Unique BVNs per Entity', index=False)
            cross_entity.to_excel(writer, sheet_name='Cross-Entity BVNs', index=False)
            entity_combinations.to_excel(writer, sheet_name='Entity Combinations', index=False)
            merged_details.to_excel(writer, sheet_name='Detailed Records', index=False)
            
        logging.info(f"Excel report saved: {filename}")
    except Exception as e:
        logging.error(f"Error saving Excel report: {str(e)}")
        sys.exit(1)

def main():
    """Main execution function."""
    setup_logging()
    
    # Configuration
    file_paths = {
        "asset-management": "./dummy-data/asset_custdata.csv",
        "trustees-digital": "./dummy-data/digital_trustees_custdata.csv",
        "trustees-traditional": "./dummy-data/traditional_trustees_custdata.csv",
        #"insurance": "./dummy-data/individual_insurance_custdata.csv",
        "registrars": "./dummy-data/registrars_shold.csv",
        "securities" : "./dummy-data/securities_custdata.csv"
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
        
        # Load and process data
        logging.info("Loading bvn_comparison.csv for cross-entity analysis.")
        df = load_data("bvn_comparison.csv")
        
        logging.info("Data loaded successfully, starting data processing")
        reports = generate_reports(df)
        
        # Save final report
        logging.info("Data processed successfully, commencing report generation.")
        save_excel_report(reports)
        
        logging.info("Analysis completed successfully")
        logging.info("Report saved to bvn_analysis_report.xlsx")

        
    except Exception as e:
        logging.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()