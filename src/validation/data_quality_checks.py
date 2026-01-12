import logging

# Expected columns for the salaries dataset
SALARIES_SCHEMA = {
    "date",
    "autonomous_community",
    "avg_salary_eur",
    "employment_rate"
}

# Expected columns for the housing dataset
HOUSING_SCHEMA = {
    "date",
    "autonomous_community",
    "avg_price_m2_eur",
    "avg_rent_m2_eur"
}

def check_schema(df, expected_schema, dataset_name):
    # Check that the dataset has the expected columns
    logging.info(f"Checking schema for {dataset_name}")
    cols = set(df.columns)

    # Columns that are missing or unexpected
    missing = expected_schema - cols
    extra = cols - expected_schema

    # Log warnings if schema does not match
    if missing:
        logging.warning(f"{dataset_name}: Missing columns: {missing}")
    if extra:
        logging.warning(f"{dataset_name}: Extra columns: {extra}")

    # Return True only if no columns are missing
    return len(missing) == 0

def check_nulls(df, dataset_name):
    # Check for null values in each column
    logging.info(f"Checking null values for {dataset_name}")
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]

    # Log which columns contain null values
    if not nulls.empty:
        logging.warning(f"{dataset_name}: Null values detected:\n{nulls}")
    else:
        logging.info(f"{dataset_name}: No null values detected")

def check_duplicates(df, dataset_name):
    # Check for duplicated rows in the dataset
    logging.info(f"Checking duplicates for {dataset_name}")
    n_dups = df.duplicated().sum()

    # Log result of duplicate check
    if n_dups > 0:
        logging.warning(f"{dataset_name}: {n_dups} duplicated rows found")
    else:
        logging.info(f"{dataset_name}: No duplicated rows found")
