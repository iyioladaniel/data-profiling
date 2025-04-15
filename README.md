# Data Profiling Project

## Overview

This project provides tools for data profiling, validation, and cross-entity analysis. It's designed to help identify and validate patterns in data across multiple sources, with a focus on handling sensitive personally identifiable information (PII) securely.

## Features

- **Data Profiling**: Generate comprehensive data profiling reports using ydata-profiling
- **Cross-Entity Validation**: Compare and validate data across multiple data sources
- **PII-Aware Processing**: Handle sensitive data with appropriate privacy measures
- **Data Hashing**: Secure handling of sensitive identifiers like BVNs and NINs
- **Data Visualization**: Streamlit apps for interactive data exploration
- **Data Validation**: Validate data fields against expected patterns

## Project Structure

```
data-profiling/
│
├── src/                              # Source code directory
│   ├── profiling/                    # Data profiling functionality
│   ├── cross_entity/                 # Cross-entity validation
│   ├── hashing/                      # Hashing functionality
│   ├── utils/                        # Utility functions
│   └── streamlit/                    # Streamlit apps
│
├── data/                             # Data directory
│   ├── raw/                          # Raw input data
│   ├── processed/                    # Processed data
│   └── dummy/                        # Generated dummy data
│
├── reports/                          # Generated reports
│   ├── html/                         # HTML reports
│   ├── json/                         # JSON reports
│   └── csv/                          # CSV reports
│
├── scripts/                          # Utility scripts
├── notebooks/                        # Jupyter notebooks
├── config/                           # Configuration files
├── docs/                             # Documentation
├── tests/                            # Test files
│
├── requirements.txt                  # Python dependencies
└── README.md                         # Project documentation
```

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/data-profiling.git
cd data-profiling
```

2. Create and activate a conda environment:
```bash
conda create -n data-profiling
conda activate data-profiling
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

### Data Profiling

To generate a profiling report for a CSV file:

```bash
python src/profiling/generate_profiling.py --csv path/to/your.csv
```

This will create an HTML report with detailed statistics about your data.

### Converting Excel to CSV

```bash
python src/profiling/convert_excel2csv.py path/to/your.xlsx
```

### Cross-Entity Validation

To compare data across multiple entities:

```bash
python src/cross_entity/base/cross_entity_validation.py
```

For PII-aware cross-entity validation:

```bash
python src/cross_entity/pii_aware/PII_aware_cross_entity_validation.py
```

### Generating Dummy Data

To generate dummy customer data:

```bash
python src/profiling/customer_data/CustomerDataIndividual.py --count 100
```

### Data Validation

To validate specific fields against patterns:

```bash
python src/utils/validity.py path/to/data.csv
```

### Interactive Visualization

Launch the Streamlit app for interactive data exploration:

```bash
streamlit run src/streamlit/app2.py
```

## Handling Sensitive Data

This project includes utilities for hashing sensitive identifiers like BVNs, NINs, and other PII:

```bash
cd src/hashing
./hash-ids.sh path/to/ids_file
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin feature/my-new-feature`
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
