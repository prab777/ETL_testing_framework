import pytest
import json
import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

# ─── Spark Session ─────────────────────────────────────────

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.master("local[*]").appName("ETLTest").getOrCreate()

# ─── Load JSON Config ──────────────────────────────────────

@pytest.fixture(scope='session')
def config():
    with open("config/rules.json") as f:
        return json.load(f)

# ─── Load Raw & Silver CSVs ────────────────────────────────

@pytest.fixture(scope='session')
def raw_df(spark):
    return spark.read.option("header", True).csv("data/raw.csv", inferSchema=True)

@pytest.fixture(scope='session')
def silver_df(spark):
    return spark.read.option("header", True).csv("data/silver.csv", inferSchema=True)

# ─── Simulated Transformed DF (raw → silver) ───────────────

@pytest.fixture(scope='session')
def transformed_df(raw_df):
    """
    Apply transformation logic:
    - Remove nulls in transaction_id or transaction_date
    - Remove rows with amount < 0
    - Convert date to yyyy-MM-dd
    - Drop duplicates on transaction_id
    """
    df = raw_df
    df = df.dropna(subset=["transaction_id", "transaction_date"])
    df = df.filter(col("transaction_amount") >= 0)
    df = df.withColumn("transaction_date", to_date("transaction_date", "yyyy-MM-dd"))
    df = df.dropDuplicates(["transaction_id"])
    return df

# ─── Log Test Results to CSV ───────────────────────────────

def pytest_terminal_summary(terminalreporter, exitstatus, config):
    results = []
    print("Logging results to CSV now")

    for report in terminalreporter.stats.get("passed", []):
        results.append(("PASSED", report.nodeid))
    for report in terminalreporter.stats.get("failed", []):
        results.append(("FAILED", report.nodeid))
    for report in terminalreporter.stats.get("error", []):
        results.append(("ERROR", report.nodeid))

    os.makedirs("results", exist_ok=True)
    with open("results/test_report.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Status", "TestCase"])
        writer.writerows(results)
    print("Logging results to CSV after")

    terminalreporter.write_sep("=", "Test results written to: results/test_report.csv")
