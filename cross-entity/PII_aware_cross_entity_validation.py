import pandas as pd
from typing import Dict, List
import logging
import sys
from itertools import combinations
import xlsxwriter

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
                columns_mapping[dataset_name]["id"]: "customer_id",
                columns_mapping[dataset_name]["bvn"]: "BVN"
            }, inplace=True)
            
            # Add dataset identifier
            df["entity"] = dataset_name
            
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
    
    logging.info(f"Before combining trustees, total number of records: {len(bvn_df)}")
    logging.info(f"Unique BVNs before combining trustees: {bvn_df['BVN'].nunique()}")
    # Combine trustees traditional & digital
    bvn_df.loc[(bvn_df.entity == "trustees-digital") | (bvn_df.entity == "trustees-traditional"), bvn_df.columns[2:]] = "trustees"
    logging.info(f"Total records after combining trustees: {len(bvn_df)}")
    logging.info(f"Unique BVNs after combining trustees: {bvn_df['BVN'].nunique()}")
    logging.info(f"Records per entity:\n{bvn_df['entity'].value_counts()}")
    
    logging.info("Moving to checking & processing duplicates.")
    # Process duplicates
    bvn_df["duplicated?"] = bvn_df["BVN"].duplicated(keep=False)
    
    # Find first occurrence of duplicated IDs
    duplicate_mapping = bvn_df[bvn_df["duplicated?"]].groupby("BVN")["serial_no"].first().to_dict()
    bvn_df["duplicated_serial_no"] = bvn_df["BVN"].map(lambda x: duplicate_mapping.get(x, ""))
    
    # Create missing BVNs DataFrame if any were found
    missing_bvn_df = pd.concat(missing_bvn_records, ignore_index=True) if missing_bvn_records else None
    
    return bvn_df, missing_bvn_df

def check_data_quality(df):
    """
    Check data quality and log potential issues
    """
    logging.info("Performing data quality checks...")
    
    # Check for nulls
    null_counts = df.isnull().sum()
    if null_counts.any():
        logging.warning(f"Found null values:\n{null_counts[null_counts > 0]}")
    
    # Check for duplicate BVNs within same entity
    entity_dups = df.groupby('entity').apply(
        lambda x: x['BVN'].duplicated().sum()
    )
    if entity_dups.any():
        logging.warning(f"Found duplicate BVNs within entities:\n{entity_dups[entity_dups > 0]}")
    
    # Log entity distribution
    logging.info(f"Records per entity:\n{df['entity'].value_counts()}")
    
    # Log BVN statistics
    logging.info(f"Total unique BVNs: {df['BVN'].nunique()}")
    logging.info(f"Total records: {len(df)}")

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

# def generate_reports(df):
#     """
#     Generate analytical reports while protecting PII (BVN) information.
#     Returns tuple of (unique_counts, cross_entity, merged_details, entity_combinations)
#     """
#     try:
#         # Unique BVNs per entity
#         unique_counts = df.groupby('entity')['BVN'].nunique().reset_index()
#         unique_counts.columns = ['Entity', 'Unique Customer Count']
        
#         # Cross-entity analysis
#         cross_entity = df.groupby('BVN').agg(
#             entity_count=('entity', 'nunique'),
#             entities=('entity', lambda x: ', '.join(sorted(x.unique())))
#         ).reset_index()
        
#         # Get the first occurrence of serial_no for each BVN
#         bvn_serial_mapping = df[['BVN', 'serial_no']].drop_duplicates(subset=['BVN'], keep='first')
        
#         # Merge to add serial_no instead of mapping
#         cross_entity = cross_entity.merge(
#             bvn_serial_mapping[['BVN', 'serial_no']], 
#             on='BVN', 
#             how='left'
#         )
        
#         # Remove BVN column after merging
#         cross_entity.drop('BVN', axis=1, inplace=True)
#         cross_entity = cross_entity[['serial_no', 'entity_count', 'entities']]
        
#         # Generate all possible entity combinations and their counts
#         unique_entities = sorted(df['entity'].unique())
#         entity_combinations = []
        
#         # For each possible number of entities
#         for i in range(2, len(unique_entities) + 1):
#             # Generate all possible combinations of that size
#             for combo in combinations(unique_entities, i):
#                 # Find serial numbers that appear in all entities in this combination
#                 mask = cross_entity['entities'].apply(
#                     lambda x: all(entity in x.split(', ') for entity in combo)
#                 )
#                 serials_in_combo = cross_entity[mask]['serial_no'].tolist()
                
#                 if serials_in_combo:  # Only add if there are matching customers
#                     entity_combinations.append({
#                         'Combination Size': i,
#                         'Entities': ' & '.join(combo),
#                         'Customer Count': len(serials_in_combo),
#                         'Serial Numbers': ', '.join(map(str, serials_in_combo))
#                     })
        
#         # Create DataFrame and sort it
#         entity_combinations_df = pd.DataFrame(entity_combinations)
#         if not entity_combinations_df.empty:
#             entity_combinations_df = entity_combinations_df.sort_values(
#                 ['Combination Size', 'Customer Count'], 
#                 ascending=[True, False]
#             )
        
#         # Detailed records with duplicate information
#         merged_details = pd.merge(
#             df[['BVN', 'entity', 'customer_id', 'serial_no', 'duplicated?', 'duplicated_serial_no']],
#             cross_entity[['serial_no', 'entity_count', 'entities']],
#             on='serial_no',
#             how='right'
#         ).sort_values(['serial_no', 'entity'])
        
#         # Remove BVN from final output
#         merged_details = merged_details.drop('BVN', axis=1)
        
#         return unique_counts, cross_entity, merged_details, entity_combinations_df
        
#     except Exception as e:
#         logging.error(f"Error generating reports: {str(e)}")
#         raise

def generate_reports(df):
    """
    Generate analytical reports while protecting PII (BVN) information.
    Memory-optimized version.
    """
    try:
        # Log initial dataframe size
        logging.info(f"Initial dataframe size: {len(df)} rows")
        
        # Unique BVNs per entity (unchanged)
        unique_counts = df.groupby('entity')['BVN'].nunique().reset_index()
        unique_counts.columns = ['Entity', 'Unique Customer Count']
        
        # Cross-entity analysis (optimized)
        logging.info("Starting cross-entity analysis...")
        cross_entity = df.groupby('BVN').agg({
            'entity': lambda x: (len(set(x)), ', '.join(sorted(set(x))))
        }).reset_index()
        
        # Rename columns after aggregation
        cross_entity[['entity_count', 'entities']] = pd.DataFrame(
            cross_entity['entity'].tolist(), 
            index=cross_entity.index
        )
        cross_entity.drop('entity', axis=1, inplace=True)
        
        # Get first serial_no for each BVN (optimized)
        bvn_serial_mapping = df.sort_values('serial_no').groupby('BVN')['serial_no'].first().reset_index()
        
        # Merge efficiently
        cross_entity = cross_entity.merge(
            bvn_serial_mapping, 
            on='BVN', 
            how='left'
        )
        
        # Remove BVN column after merging
        cross_entity.drop('BVN', axis=1, inplace=True)
        cross_entity = cross_entity[['serial_no', 'entity_count', 'entities']]
        
        logging.info(f"Cross-entity analysis complete. Found {len(cross_entity)} unique BVNs")
        
        # Generate entity combinations more efficiently
        unique_entities = sorted(df['entity'].unique())
        entity_combinations = []
        
        logging.info("Generating entity combinations...")
        
        # Process in smaller batches for memory efficiency
        batch_size = 1000
        for i in range(2, len(unique_entities) + 1):
            combo_batches = [
                list(combinations(unique_entities, i))[j:j+batch_size]
                for j in range(0, len(list(combinations(unique_entities, i))), batch_size)
            ]
            
            for combo_batch in combo_batches:
                for combo in combo_batch:
                    mask = cross_entity['entities'].apply(
                        lambda x: all(entity in x.split(', ') for entity in combo)
                    )
                    matching_serials = cross_entity.loc[mask, 'serial_no'].tolist()
                    
                    if matching_serials:
                        entity_combinations.append({
                            'Combination Size': i,
                            'Entities': ' & '.join(combo),
                            'Customer Count': len(matching_serials),
                            'Serial Numbers': ', '.join(map(str, matching_serials[:1000]))  # Limit serial numbers to prevent excessive size
                        })
        
        logging.info(f"Found {len(entity_combinations)} valid entity combinations")
        
        # Create DataFrame and sort it
        entity_combinations_df = pd.DataFrame(entity_combinations)
        if not entity_combinations_df.empty:
            entity_combinations_df = entity_combinations_df.sort_values(
                ['Combination Size', 'Customer Count'], 
                ascending=[True, False]
            )
        
        # Optimize merged_details creation
        logging.info("Creating merged details...")
        merged_details = df[['BVN', 'entity', 'customer_id', 'serial_no', 'duplicated?', 'duplicated_serial_no']].copy()
        merged_details = merged_details.merge(
            cross_entity[['serial_no', 'entity_count', 'entities']],
            on='serial_no',
            how='left'
        )
        merged_details.sort_values(['serial_no', 'entity'], inplace=True)
        merged_details.drop('BVN', axis=1, inplace=True)
        
        logging.info(f"Final merged_details size: {len(merged_details)} rows")
        
        return unique_counts, cross_entity, merged_details, entity_combinations_df
        
    except Exception as e:
        logging.error(f"Error generating reports: {str(e)}")
        raise

def save_to_csv(dataframe):
    """Save large records of dataframe to csv"""

def save_excel_report(results, filename='PII_aware_bvn_analysis_report.xlsx'):
    """
    Save all results to Excel with multiple sheets, properly handling large datasets
    by splitting them into multiple sheets when necessary.
    """
    try:
        unique_counts, cross_entity, merged_details, entity_combinations = results
        max_rows = 1_000_000  # Slightly below Excel's limit for safety
        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            # Save smaller DataFrames normally
            unique_counts.to_excel(writer, sheet_name='Unique BVNs per Entity', index=False)
            cross_entity.to_excel(writer, sheet_name='Cross-Entity BVNs', index=False)
            entity_combinations.to_excel(writer, sheet_name='Entity Combinations', index=False)
            
            # Handle merged_details with chunking
            total_rows = len(merged_details)
            if total_rows > max_rows:
                logging.info(f"merged_records is greater than max excel rows, so saving the merged_details dataframe to csv.")
                merged_details.to_csv("merged_detailed_records.csv", index=False)
                # num_chunks = (total_rows // max_rows) + 1
                # logging.info(f"Splitting detailed records into {num_chunks} sheets")
                
                # for chunk_idx in range(num_chunks):
                #     start_idx = chunk_idx * max_rows
                #     end_idx = min((chunk_idx + 1) * max_rows, total_rows)
                    
                #     chunk = merged_details.iloc[start_idx:end_idx]
                #     sheet_name = f'Detailed Records_{chunk_idx + 1}'
                    
                #     # Add chunk range information to help users
                #     chunk.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                #     # Get the worksheet object
                #     worksheet = writer.sheets[sheet_name]
                #     # Add a note about the range
                #     worksheet.write(0, len(merged_details.columns) + 1, 
                #                  f'Records {start_idx + 1} to {end_idx}')
                
                # # Create a summary sheet
                # summary = pd.DataFrame({
                #     'Sheet Name': [f'Detailed Records_{i+1}' for i in range(num_chunks)],
                #     'Record Range': [f'Records {i*max_rows + 1} to {min((i+1)*max_rows, total_rows)}' 
                #                    for i in range(num_chunks)],
                #     'Number of Records': [min(max_rows, total_rows - i*max_rows) 
                #                         for i in range(num_chunks)]
                # })
                # summary.to_excel(writer, sheet_name='Detailed Records Guide', index=False)
                
            else:
                # If the data fits in one sheet, save it normally
                merged_details.to_excel(writer, sheet_name='Detailed Records', index=False)
        
        logging.info(f"Excel report saved successfully to {filename}")
        
    except Exception as e:
        logging.error(f"Error saving Excel report: {str(e)}")
        raise

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
        
        # Add data quality check
        check_data_quality(bvn_df)
        
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