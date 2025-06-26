from pyspark.sql.functions import col


def test_dummy():
    print("hihhhi")
    assert (1 == 1 ), "wrong"

def test_no_nulls_in_required_columns(silver_df, config):
    for col_name in config["required_columns"]:
        null_count = silver_df.filter(col(col_name).isNull()).count()
        assert null_count == 0, f"Nulls found in column: {col_name}"

def test_no_duplicates_on_primary_key(silver_df, config):
    pk = config["primary_key"]
    total = silver_df.count()
    unique = silver_df.dropDuplicates(pk).count()
    assert total == unique, f"Duplicate rows found on primary key: {pk}"

def test_business_rules(silver_df, config):
    for rule in config["business_rules"]:
        failed_count = silver_df.filter(f"NOT ({rule['expression']})").count()
        assert failed_count == 0, f"Rule failed: {rule['name']} — {failed_count} rows"

def test_record_count_match(transformed_df, silver_df):
    assert transformed_df.count() == silver_df.count(), "Row count mismatch between transformed_df and silver"


def test_loaded_data_matches_transformed(transformed_df, silver_df, config):
    """Validate that data loaded into silver matches transformed raw data."""
    pk = config["primary_key"]
    transformed = transformed_df.dropna(subset=pk).dropDuplicates(pk)
    silver = silver_df.dropna(subset=pk).dropDuplicates(pk)

    mismatched = transformed.join(silver, pk, "outer")        .filter(" OR ".join([f"(transformed.{col} IS NULL OR silver.{col} IS NULL OR transformed.{col} != silver.{col})"
                             for col in transformed.columns if col in silver.columns]))        .alias("transformed").join(silver.alias("silver"), pk)

    assert mismatched.count() == 0, "Data mismatch found between transformed raw and silver layer"
