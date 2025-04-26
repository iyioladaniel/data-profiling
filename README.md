# Data Profiling Project

A comprehensive tool for data profiling, validation, and cross-entity analysis with support for secure handling of personally identifiable information (PII).

## Current Project Structure

```
data-profiling/
├── config/                         # Configuration files
├── data/                           # Data files
├── reports/                        # Generated reports
├── scripts/                        # Scripts
│   ├── cross-entity/               # Cross-entity validation scripts
│   │   ├── multiple-ids/           # Multiple ID validation
│   │   ├── cross_entity_validation.py
│   │   ├── dynamic_hashed_cross_entity_analysis.py
│   │   ├── hashed_cross_entity_analysis.py
│   │   ├── PII_aware_cross_entity_validation.py
│   │   └── PII_aware_NIN_cross_entity_validation.py
│   ├── generate-dummy-data/        # Data generation scripts
│   │   ├── CustomerDataCorporate.py
│   │   ├── CustomerDataIndividual.py
│   │   └── generate_random.py
│   ├── hashing/                    # PII hashing utilities
│   ├── duplicate_bvn_analysis.py
│   ├── generate_profiling.py
│   └── validity_of_national_ids.py
├── streamlit/                      # Streamlit applications
│   ├── app.py
│   ├── app2.py
│   └── data_inventory_app.py
├── tests/                          # Test files
├── utils/                          # Utility scripts
│   └── convert_excel2csv.py
├── requirements.txt                # Python dependencies
└── README.md                       # Project documentation
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
python scripts/generate_profiling.py --csv path/to/your.csv
```

This will create an HTML report with detailed statistics about your data.

### Converting Excel to CSV

```bash
python utils/convert_excel2csv.py path/to/your.xlsx
```

### Cross-Entity Validation

To compare data across multiple entities:

```bash
python scripts/cross-entity/cross_entity_validation.py
```

For PII-aware cross-entity validation:

```bash
python scripts/cross-entity/PII_aware_cross_entity_validation.py
```

### Generating Dummy Data

To generate dummy customer data:

```bash
python scripts/generate-dummy-data/CustomerDataIndividual.py --count 100
```

### Data Validation

To validate specific fields against patterns:

```bash
python scripts/validity_of_national_ids.py path/to/data.csv
```

The validation includes checks for:
- Nigerian NIN (11 digits)
- BVN (11 digits starting with 22)
- Nigerian passport numbers
- Driver's license numbers

### Interactive Visualization

Launch the Streamlit app for interactive data exploration:

```bash
streamlit run streamlit/app2.py
```

## Handling Sensitive Data

This project includes utilities for hashing sensitive identifiers like BVNs, NINs, and other PII in the `scripts/hashing/` directory.

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin feature/my-new-feature`
5. Submit a pull request

## License

This project is licensed under the MIT License.
