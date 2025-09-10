import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session for testing
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("Synthea ETL Tests") \
        .master("local[1]") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def df(spark):
    # Read the cleaned parquet dataset
    return spark.read.parquet("data/clean/patients_with_encounters")

# Test 1: NO NULL patient_id
def test_no_null_patient_id(df):
    assert df.filter(col("patient_id").isNull()).count() == 0, "Found null patient_id"

# Test 2: encounters non-negative
def test_encounters_non_negative(df):
    assert df.filter(col("num_encounters") < 0).count() == 0, "Found negative encounters"
