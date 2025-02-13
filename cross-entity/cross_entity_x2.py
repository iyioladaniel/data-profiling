import pandas as pd
from itertools import combinations
import logging
import sys
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bvn_analysis.log'),
        logging.StreamHandler()
    ]
)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='BVN Cross-Entity Analysis')
    parser.add_argument('csv_file', help='Path to the input CSV file')
    return parser.parse_args()

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
    """Main execution flow"""
    args = parse_arguments()
    logging.info(f"Starting analysis for: {args.csv_file}")
    
    # Load and process data
    df = load_data(args.csv_file)
    reports = generate_reports(df)
    
    # Save final report
    save_excel_report(reports)
    
    logging.info("Analysis completed successfully")

if __name__ == '__main__':
    main()