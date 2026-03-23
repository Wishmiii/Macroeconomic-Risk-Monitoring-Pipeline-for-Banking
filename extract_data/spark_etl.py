import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from awsglue.utils import getResolvedOptions

spark = SparkSession.builder.appName("FRED_ETL").getOrCreate()

args = getResolvedOptions(sys.argv, ["RUN_DATE"])
RUN_DATE = args["RUN_DATE"]

INPUT_PATH = f"s3://fred-banking-data-lake/processed/fred/run_date={RUN_DATE}/combined_wide.csv"

OUTPUT_PATH = f"s3://fred-banking-data-lake/analytics/fred/run_date={RUN_DATE}/"

df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

# Rename columns for clarity
df = df.withColumnRenamed("UNRATE", "unemployment") \
       .withColumnRenamed("CPIAUCSL", "inflation") \
       .withColumnRenamed("FEDFUNDS", "interest_rate") \
       .withColumnRenamed("GDP", "gdp")

# data cleaning
# Handle missing values
df = df.fillna(0)

# feature engineering
# Risk score formula 
df = df.withColumn(
    "risk_score",
    (col("unemployment") * 0.4) +
    (col("inflation") * 0.3) +
    (col("interest_rate") * 0.2) -
    (col("gdp") * 0.1)
)

# Risk classification
df = df.withColumn(
    "risk_level",
    when(col("risk_score") > 10, "HIGH")
    .when(col("risk_score") > 5, "MEDIUM")
    .otherwise("LOW")
)

# save output
df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("ETL job completed successfully!")