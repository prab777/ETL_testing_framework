from pyspark.sql.functions import max as spark_max

def test_max_transaction_date_not_regressed(raw_df, silver_df, config):
    raw_max = raw_df.selectExpr(f"max({config['date_column']})").collect()[0][0]
    silver_max = silver_df.selectExpr(f"max({config['date_column']})").collect()[0][0]
    assert raw_max >= silver_max, f"Raw max date {raw_max} is older than silver max date {silver_max}"

def test_no_reappearing_history(raw_df, silver_df, config):
    pk = config["primary_key"]
    dupes = raw_df.join(silver_df, pk, "inner").count()
    assert dupes == 0, f"{dupes} historic rows reappeared in silver"

def test_new_records_inserted(raw_df, silver_df, config):
    pk = config["primary_key"]
    raw_only = raw_df.join(silver_df, pk, "leftanti")
    new_count = raw_only.count()
    assert new_count > 0, "No new rows were inserted — check ETL logic or duplicate re-load"