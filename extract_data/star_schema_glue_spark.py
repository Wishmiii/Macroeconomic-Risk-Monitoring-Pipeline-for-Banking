import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, quarter,
    date_format, when, avg
)
from pyspark.sql.window import Window
from datetime import datetime
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["RUN_DATE"])
RUN_DATE = args["RUN_DATE"]


spark = SparkSession.builder.appName("FRED_STAR_SCHEMA").getOrCreate()

args = getResolvedOptions(sys.argv, ["RUN_DATE"])
RUN_DATE = args["RUN_DATE"]


INPUT_PATH = f"s3://fred-banking-data-lake/processed/fred/run_date={RUN_DATE}/combined_wide.csv"

# read the csv to a spark dataframe
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

df = df.withColumn("date", to_date(col("date")))

# dimention table - dim_date
dim_date = (
    df.select("date")
    .dropDuplicates()
    .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("quarter", quarter(col("date")))
    .select("date_key", col("date").alias("full_date"), "year", "month", "quarter")
)

# handle missing values
df = df.fillna(0)

# rename columns
df = (
    df.withColumnRenamed("UNRATE", "unemployment")
      .withColumnRenamed("CPIAUCSL", "inflation")
      .withColumnRenamed("FEDFUNDS", "interest_rate")
      .withColumnRenamed("GDP", "gdp")
)

# feature engineering
window = Window.orderBy("date").rowsBetween(-2, 0)

df = df.withColumn("unemployment_avg", avg("unemployment").over(window))
df = df.withColumn("interest_rate_avg", avg("interest_rate").over(window))

# risk score formula
df = df.withColumn(
    "risk_score",
    (col("unemployment_avg") * 0.5) +
    (col("interest_rate_avg") * 0.3) +
    (col("inflation") * 0.2)
)

# dimention table - dim_risk_level
dim_risk_level = spark.createDataFrame([
    (1, "LOW", "Stable economy"),
    (2, "MEDIUM", "Moderate risk"),
    (3, "HIGH", "High risk")
], ["risk_level_key", "risk_level", "description"])

# Assign risk levels
df = df.withColumn(
    "risk_level_key",
    when(col("risk_score") > 10, 3)
    .when(col("risk_score") > 5, 2)
    .otherwise(1)
)

# fact table
fact = (
    df.join(dim_date, df.date == dim_date.full_date)
    .select(
        "date_key",
        "risk_level_key",
        "unemployment",
        "inflation",
        "interest_rate",
        "gdp",
        "risk_score"
    )
)

# write output to s3
BASE_PATH = "s3://fred-banking-data-lake/datamart/"

dim_date.write.mode("overwrite").parquet(BASE_PATH + "dim_date/")
dim_risk_level.write.mode("overwrite").parquet(BASE_PATH + "dim_risk_level/")
fact.write.mode("overwrite").parquet(BASE_PATH + "fact_banking_macro_risk/")

print("Star schema created successfully!")