import pandas as pd
from typing import Dict, List, Tuple
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

def analyze_id_duplicates(
    file_paths: Dict[str, str],
    columns_mapping: Dict[str, Dict[str, Dict[str, str]]],
    id_types: List[str]
) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Analyze duplicates across multiple datasets for multiple ID types.
    
    Args:
        file_paths: Dictionary mapping dataset names to file paths
        columns_mapping: Dictionary mapping dataset names to their column mappings for each ID type
        id_types: List of ID types to analyze (e.g., ['NIN', 'PASSPORT', 'DRIVERS_LICENSE'])
        
    Returns:
        Dictionary mapping ID types to tuples of (analysis DataFrame, missing records DataFrame)
    """
    results = {}
    
    for id_type in id_types:
        logging.info(f"\nStarting {id_type} duplicate analysis")
        
        id_records: List[pd.DataFrame] = []
        missing_records: List[pd.DataFrame] = []
        
        # Process each dataset
        for dataset_name, file_path in file_paths.items():
            try:
                if id_type not in columns_mapping[dataset_name]:
                    logging.info(f"Skipping {dataset_name} for {id_type} - no mapping found")
                    continue
                    
                logging.info(f"Processing dataset: {dataset_name} for {id_type}")
                
                # Read dataset with only required columns
                df = pd.read_csv(
                    file_path,
                    usecols=[
                        columns_mapping[dataset_name][id_type]["id"],
                        columns_mapping[dataset_name][id_type]["value"]
                    ]
                )
                
                # Rename columns for consistency
                df.rename(columns={
                    columns_mapping[dataset_name][id_type]["id"]: "customer_id",
                    columns_mapping[dataset_name][id_type]["value"]: id_type
                }, inplace=True)
                
                # Add dataset identifier
                df["entity"] = dataset_name
                
                # Handle missing values
                missing_mask = (df[id_type].isna()) | (df[id_type]=="-")
                if missing_mask.any():
                    missing_record = df[missing_mask].copy()
                    missing_record["reason"] = f"Missing {id_type}"
                    missing_records.append(missing_record)
                    logging.warning(f"Found {missing_mask.sum()} missing {id_type}s in {dataset_name}")
                
                # Remove rows with missing values for main analysis
                df = df[~missing_mask]
                
                id_records.append(df)
                
            except Exception as e:
                logging.error(f"Error processing {dataset_name} for {id_type}: {str(e)}")
                continue
        
        if not id_records:
            logging.warning(f"No valid data found for {id_type}")
            continue
        
        # Combine all valid records
        combined_df = pd.concat(id_records, ignore_index=True)
        
        # Create serial id column
        combined_df["serial_no"] = combined_df.index + 1
        
        logging.info(f"Before processing trustees - {id_type}:")
        logging.info(f"Total records: {len(combined_df)}")
        logging.info(f"Unique {id_type}s: {combined_df[id_type].nunique()}")
        
        # Combine trustees if present
        if "trustees-digital" in combined_df["entity"].values or "trustees-traditional" in combined_df["entity"].values:
            combined_df.loc[
                (combined_df.entity == "trustees-digital") | 
                (combined_df.entity == "trustees-traditional"),
                "entity"
            ] = "trustees"
            
            logging.info(f"After combining trustees - {id_type}:")
            logging.info(f"Total records: {len(combined_df)}")
            logging.info(f"Unique {id_type}s: {combined_df[id_type].nunique()}")
        
        logging.info(f"Records per entity for {id_type}:\n{combined_df['entity'].value_counts()}")
        
        # Process duplicates
        combined_df["duplicated?"] = combined_df[id_type].duplicated(keep=False)
        
        # Find first occurrence of duplicated IDs
        duplicate_mapping = combined_df[combined_df["duplicated?"]].groupby(id_type)["serial_no"].first().to_dict()
        combined_df["duplicated_serial_no"] = combined_df[id_type].map(lambda x: duplicate_mapping.get(x, ""))
        
        # Create missing records DataFrame if any were found
        missing_df = pd.concat(missing_records, ignore_index=True) if missing_records else None
        
        results[id_type] = (combined_df, missing_df)
    
    return results

def check_data_quality(df: pd.DataFrame, id_type: str):
    """
    Check data quality and log potential issues
    """
    logging.info(f"\nPerforming data quality checks for {id_type}...")
    
    # Check for nulls
    null_counts = df.isnull().sum()
    if null_counts.any():
        logging.warning(f"Found null values:\n{null_counts[null_counts > 0]}")
    
    # Check for duplicate IDs within same entity
    entity_dups = df.groupby('entity').apply(
        lambda x: x[id_type].duplicated().sum()
    )
    if entity_dups.any():
        logging.warning(f"Found duplicate {id_type}s within entities:\n{entity_dups[entity_dups > 0]}")
    
    # Log entity distribution
    logging.info(f"Records per entity:\n{df['entity'].value_counts()}")
    
    # Log ID statistics
    logging.info(f"Total unique {id_type}s: {df[id_type].nunique()}")
    logging.info(f"Total records: {len(df)}")

def generate_reports(df: pd.DataFrame, id_type: str):
    """
    Generate analytical reports while protecting PII.
    Memory-optimized version.
    """
    try:
        logging.info(f"\nGenerating reports for {id_type}")
        logging.info(f"Initial dataframe size: {len(df)} rows")
        
        # Unique IDs per entity
        unique_counts = df.groupby('entity')[id_type].nunique().reset_index()
        unique_counts.columns = ['Entity', f'Unique {id_type} Count']
        
        # Cross-entity analysis
        logging.info("Starting cross-entity analysis...")
        cross_entity = df.groupby(id_type).agg({
            'entity': lambda x: (len(set(x)), ', '.join(sorted(set(x))))
        }).reset_index()
        
        # Rename columns after aggregation
        cross_entity[['entity_count', 'entities']] = pd.DataFrame(
            cross_entity['entity'].tolist(), 
            index=cross_entity.index
        )
        
        # Get first serial_no for each ID
        id_serial_mapping = df.sort_values('serial_no').groupby(id_type)['serial_no'].first().reset_index()
        
        # Merge efficiently
        cross_entity = cross_entity.merge(
            id_serial_mapping, 
            on=id_type, 
            how='left'
        )
        
        # Remove ID column after merging
        cross_entity.drop('entity', axis=1, inplace=True)
        cross_entity = cross_entity[['serial_no', 'entity_count', 'entities']]
        
        logging.info(f"Cross-entity analysis complete. Found {len(cross_entity)} unique {id_type}s")
        
        # Generate entity combinations
        entity_combinations = process_entity_combinations(cross_entity, df)
        
        # Create merged details
        merged_details = create_merged_details(df, cross_entity, id_type)
        
        return unique_counts, cross_entity, merged_details, entity_combinations
        
    except Exception as e:
        logging.error(f"Error generating reports for {id_type}: {str(e)}")
        raise

def process_entity_combinations(cross_entity: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Process entity combinations efficiently"""
    unique_entities = sorted(df['entity'].unique())
    entity_combinations = []
    
    logging.info("Generating entity combinations...")
    
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
                        'Serial Numbers': ', '.join(map(str, matching_serials[:1000]))
                    })
    
    combinations_df = pd.DataFrame(entity_combinations)
    if not combinations_df.empty:
        combinations_df.sort_values(
            ['Combination Size', 'Customer Count'], 
            ascending=[True, False],
            inplace=True
        )
    
    return combinations_df

def create_merged_details(df: pd.DataFrame, cross_entity: pd.DataFrame, id_type: str) -> pd.DataFrame:
    """Create merged details DataFrame"""
    logging.info("Creating merged details...")
    merged_details = df[[id_type, 'entity', 'customer_id', 'serial_no', 'duplicated?', 'duplicated_serial_no']].copy()
    merged_details = merged_details.merge(
        cross_entity[['serial_no', 'entity_count', 'entities']],
        on='serial_no',
        how='left'
    )
    merged_details.sort_values(['serial_no', 'entity'], inplace=True)
    merged_details.drop(id_type, axis=1, inplace=True)
    
    logging.info(f"Final merged_details size: {len(merged_details)} rows")
    return merged_details

def save_excel_report(results: Dict[str, tuple], filename='cross_entity_analysis_report.xlsx'):
    """Save all results to Excel with multiple sheets"""
    try:
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            for id_type, (unique_counts, cross_entity, merged_details, entity_combinations) in results.items():
                # Save smaller DataFrames
                sheet_prefix = f"{id_type}_"
                unique_counts.to_excel(writer, sheet_name=f'{sheet_prefix}Unique_Counts', index=False)
                cross_entity.to_excel(writer, sheet_name=f'{sheet_prefix}Cross_Entity', index=False)
                entity_combinations.to_excel(writer, sheet_name=f'{sheet_prefix}Combinations', index=False)
                
                # Handle large merged_details
                if len(merged_details) > 1_000_000:
                    merged_details.to_csv(f"{id_type}_merged_detailed_records.csv", index=False)
                    logging.info(f"{id_type} merged_details saved to CSV due to size")
                else:
                    merged_details.to_excel(writer, sheet_name=f'{sheet_prefix}Details', index=False)
        
        logging.info(f"Excel report saved successfully to {filename}")
        
    except Exception as e:
        logging.error(f"Error saving Excel report: {str(e)}")
        raise

def main():
    """Main execution function."""
    setup_logging()
    
    # Configuration
    file_paths = {
        "trustees": "./digital_trustees_custdata_pivot.csv",
        "insurance": "../dummy-data/individual_insurance_custdata.csv"
    }
    
    # Updated column mapping structure for multiple ID types
    columns_mapping = {
        "trustees": {
            "NIN": {"id": "platformuserid", "value": "nin"},
            "PASSPORT": {"id": "platformuserid", "value": "passport"},
            "DRIVERS_LICENSE": {"id": "platformuserid", "value": "dri"}
        },
        "insurance": {
            "NIN": {"id": "Customer ID", "value": "NIN"},
            "PASSPORT": {"id": "Customer ID", "value": "Passport Number"},
            "DRIVERS_LICENSE": {"id": "Customer ID", "value": "Driver Licence"}
        }
    }
    
    # List of ID types to analyze
    id_types = ["NIN", "PASSPORT", "DRIVERS_LICENSE"]
    
    try:
        # Perform analysis for all ID types
        analysis_results = analyze_id_duplicates(file_paths, columns_mapping, id_types)
        
        # Process each ID type
        report_results = {}
        for id_type, (id_df, missing_df) in analysis_results.items():
            # Save initial analysis
            id_df.to_csv(f"{id_type.lower()}_comparison.csv", index=False)
            if missing_df is not None:
                missing_df.to_csv(f"missing_{id_type.lower()}.csv", index=False)
            
            # Check data quality
            check_data_quality(id_df, id_type)
            
            # Generate reports
            reports = generate_reports(id_df, id_type)
            report_results[id_type] = reports
        
        # Save final report
        save_excel_report(report_results)
        
        logging.info("Analysis completed successfully")
        
    except Exception as e:
        logging.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()