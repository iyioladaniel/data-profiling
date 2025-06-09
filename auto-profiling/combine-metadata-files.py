#!/usr/bin/env python3
"""
combine-metadata-files.py - Combine all metadata_report.csv files into a single file

This script recursively searches through a folder structure to find all
metadata_report.csv files and combines them into a single comprehensive CSV file.
It handles files with different column structures, ensuring the core required
columns are always included.

Usage:
    python combine-metadata-files.py source_folder [output_directory]
"""

import os
import sys
import pandas as pd
from datetime import datetime
import logging
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Key columns that should be present in all metadata files
CORE_COLUMNS = [
    'source', 'table_name', 'column_name', 'data_type', 'is_sensitive',
    'total_records', 'total_count', 'missing_count', 'completeness_pct',
    'is_unique', 'unique_count', 'unique_percentage', 'distinct_count', 
    'distinct_percentage', 'category_count', 'position', 'data_length', 
    'data_precision', 'data_scale', 'nullable', 'required', 'comments', 
    'created_at', 'last_updated'
]

def find_metadata_files(source_folder):
    """
    Recursively find all metadata_report.csv files in the source folder
    
    Args:
        source_folder: Root directory to search
        
    Returns:
        List of full paths to metadata_report.csv files
    """
    metadata_files = []
    
    logger.info(f"Searching for metadata_report.csv files in {source_folder}")
    
    for root, _, files in os.walk(source_folder):
        for file in files:
            if file == 'metadata_report.csv':
                full_path = os.path.join(root, file)
                metadata_files.append(full_path)
                logger.info(f"Found metadata file: {full_path}")
    
    logger.info(f"Found {len(metadata_files)} metadata files in total")
    return metadata_files

def combine_metadata_files(file_list, output_dir=None):
    """
    Combine all metadata files into a single DataFrame
    
    Args:
        file_list: List of paths to metadata_report.csv files
        output_dir: Directory to save the combined file (optional)
        
    Returns:
        Path to the saved combined file
    """
    if not file_list:
        logger.error("No metadata files found to combine")
        return None
    
    all_dfs = []
    error_files = []
    
    # Process each file
    for file_path in file_list:
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Add file source information
            df['file_source'] = file_path
            
            # Check for required columns and fill missing ones with None
            for col in CORE_COLUMNS:
                if col not in df.columns:
                    df[col] = None
                    logger.warning(f"Added missing column '{col}' to file {file_path}")
            
            # Add to list of DataFrames
            all_dfs.append(df)
            logger.info(f"Processed {file_path}: {len(df)} rows")
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            error_files.append((file_path, str(e)))
    
    if not all_dfs:
        logger.error("Failed to process any metadata files")
        return None
    
    # Combine all DataFrames
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Ensure all core columns are included first, followed by any additional columns
    all_columns = CORE_COLUMNS + [col for col in combined_df.columns if col not in CORE_COLUMNS]
    combined_df = combined_df[all_columns]
    
    # Generate output path
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"combined_metadata_{timestamp}.csv")
    else:
        output_path = f"combined_metadata_{timestamp}.csv"
    
    # Save combined file
    combined_df.to_csv(output_path, index=False)
    logger.info(f"Combined metadata saved to {output_path}")
    logger.info(f"Total rows: {len(combined_df)}")
    logger.info(f"Total unique tables: {combined_df['table_name'].nunique()}")
    
    # Report any errors
    if error_files:
        logger.warning(f"Errors occurred in {len(error_files)} files:")
        for file_path, error in error_files:
            logger.warning(f"  - {file_path}: {error}")
    
    return output_path

def main():
    """Main execution function with command-line argument parsing"""
    # Set up argument parser with detailed help
    parser = argparse.ArgumentParser(
        description="Combine metadata_report.csv files from multiple directories into a single file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python combine-metadata-files.py /path/to/source
  python combine-metadata-files.py /path/to/source /path/to/output
  python combine-metadata-files.py --log-level DEBUG /path/to/source
        """
    )
    
    # Add arguments
    parser.add_argument(
        "source_folder", 
        help="Root directory to search for metadata_report.csv files"
    )
    parser.add_argument(
        "output_dir", 
        nargs="?", 
        default=None, 
        help="Directory to save the combined metadata file (optional)"
    )
    parser.add_argument(
        "--log-level", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: INFO)"
    )
    parser.add_argument(
        "--version", 
        action="version", 
        version="%(prog)s 1.0.0"
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set logging level from command line argument
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    if not os.path.isdir(args.source_folder):
        logger.error(f"Source folder does not exist: {args.source_folder}")
        return 1
    
    # Find and combine metadata files
    metadata_files = find_metadata_files(args.source_folder)
    if not metadata_files:
        logger.error("No metadata files found")
        return 1
    
    output_path = combine_metadata_files(metadata_files, args.output_dir)
    if output_path:
        logger.info("Successfully combined metadata files")
        return 0
    else:
        logger.error("Failed to combine metadata files")
        return 1

if __name__ == "__main__":
    sys.exit(main())