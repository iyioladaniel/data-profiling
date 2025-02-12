import pandas as pd
import matplotlib.pyplot as plt
from matplotlib_venn import venn2, venn3
from itertools import combinations
import logging
import os
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

def create_venn_diagrams(cross_entity_df, output_dir='venn_diagrams'):
    """
    Create Venn diagrams for entity overlaps
    Creates both 2-way and 3-way combinations
    """
    try:
        # Extract unique entities
        all_entities = set()
        for ent_list in cross_entity_df['entities']:
            all_entities.update(ent_list.split(', '))
        unique_entities = sorted(all_entities)
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logging.info(f"Created output directory: {output_dir}")

        # Generate 2-way combinations
        logging.info("Generating 2-way Venn diagrams")
        for pair in combinations(unique_entities, 2):
            _create_venn(cross_entity_df, pair, output_dir)

        # Generate 3-way combinations
        logging.info("Generating 3-way Venn diagrams")
        for triple in combinations(unique_entities, 3):
            _create_venn(cross_entity_df, triple, output_dir)

    except Exception as e:
        logging.error(f"Error creating Venn diagrams: {str(e)}")

def _create_venn(cross_entity_df, entities, output_dir):
    """Helper function to create individual Venn diagrams"""
    try:
        entity_sets = {}
        for entity in entities:
            mask = cross_entity_df['entities'].str.contains(entity)
            entity_sets[entity] = set(cross_entity_df[mask]['BVN'])
        
        plt.figure(figsize=(12, 8))
        
        if len(entities) == 2:
            venn2(
                [entity_sets[entities[0]], entity_sets[entities[1]]],
                set_labels=entities
            )
            plt.title(f"BVN Overlap: {entities[0]} vs {entities[1]}")
            filename = f"2way_{entities[0]}_{entities[1]}_bvn_overlap.png"
        else:  # 3 entities
            venn3(
                [entity_sets[entities[0]], entity_sets[entities[1]], entity_sets[entities[2]]],
                set_labels=entities
            )
            plt.title(f"BVN Overlap: {', '.join(entities)}")
            filename = f"3way_{'_'.join(entities)}_bvn_overlap.png"
        
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, bbox_inches='tight')
        plt.close()
        logging.info(f"Saved Venn diagram: {filename}")
        
    except Exception as e:
        logging.error(f"Failed to create Venn diagram for {entities}: {str(e)}")

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
        
        # Add occurrence count per combination
        entity_combinations = []
        for i in range(2, len(df['entity'].unique()) + 1):
            for combo in combinations(sorted(df['entity'].unique()), i):
                mask = cross_entity['entities'].apply(
                    lambda x: all(entity in x.split(', ') for entity in combo)
                )
                count = mask.sum()
                if count > 0:
                    entity_combinations.append({
                        'Entities': ' & '.join(combo),
                        'Number of BVNs': count,
                        'Entity Count': i
                    })
        
        entity_combinations_df = pd.DataFrame(entity_combinations)
        
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
        sys.exit(1)

def save_excel_report(results, filename='bvn_analysis_report.xlsx'):
    """Save all results to Excel with multiple sheets"""
    try:
        unique_counts, cross_entity, merged_details, entity_combinations = results
        
        with pd.ExcelWriter(filename) as writer:
            unique_counts.to_excel(writer, sheet_name='Unique BVNs per Entity', index=False)
            cross_entity.to_excel(writer, sheet_name='Cross-Entity BVNs', index=False)
            merged_details.to_excel(writer, sheet_name='Detailed Records', index=False)
            entity_combinations.to_excel(writer, sheet_name='Entity Combinations', index=False)
            
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
    
    # Create visualizations
    create_venn_diagrams(reports[1])
    
    # Save final report
    save_excel_report(reports)
    
    logging.info("Analysis completed successfully")

if __name__ == '__main__':
    main()