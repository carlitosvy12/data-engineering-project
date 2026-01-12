import logging
from src.ingestion.load_raw_data import load_salaries_data, load_housing_data
from src.validation.data_quality_checks import (
    SALARIES_SCHEMA, HOUSING_SCHEMA,
    check_schema, check_nulls, check_duplicates
)

# Basic logging configuration for console output
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

if __name__ == "__main__":
    # Load raw datasets
    salaries_df = load_salaries_data()
    housing_df = load_housing_data()

    # Run quality checks on salaries data
    check_schema(salaries_df, SALARIES_SCHEMA, "Salaries")
    check_nulls(salaries_df, "Salaries")
    check_duplicates(salaries_df, "Salaries")

    # Run quality checks on housing data
    check_schema(housing_df, HOUSING_SCHEMA, "Housing")
    check_nulls(housing_df, "Housing")
    check_duplicates(housing_df, "Housing")
