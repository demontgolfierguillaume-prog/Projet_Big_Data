"""
Copy to include/team_<yourname>_spark.py and implement three Spark transformations.

Spec: read silver with schema → enrich → aggregate → write curated Parquet + dashboard JSON.
The smoke-test Spark code is baked into the Docker image (not shipped in the student kit).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from include.paths import raw_parquet, curated_kpis, report_json


# =========================
# TRANSFORM 1 - READ + CLEAN
# =========================
def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:

    path = str(raw_parquet(logical_date))
    df = spark.read.parquet(path)

    # cast
    df = df.withColumn("amount_eur", F.col("amount_eur").cast("double"))

    # data quality (TP style)
    df = df.filter(
        (F.col("amount_eur").isNotNull())
        & (F.col("amount_eur") > 0)
        & (F.col("country").isNotNull())
        & (F.col("category").isNotNull())
    )

    # outliers removal
    df = df.filter(F.col("amount_eur") < 5000)

    # timestamp features
    df = df.withColumn("ts", F.to_timestamp("ts"))
    df = df.withColumn("hour", F.hour("ts"))
    df = df.withColumn("day", F.to_date("ts"))

    return df


# =========================
# TRANSFORM 2 - ENRICH
# =========================
def transform_2(df: DataFrame, logical_date: str) -> DataFrame:

    # segmentation montant
    df = df.withColumn(
        "amount_bucket",
        F.when(F.col("amount_eur") < 50, "low")
        .when(F.col("amount_eur") < 200, "mid")
        .when(F.col("amount_eur") < 1000, "high")
        .otherwise("vip"),
    )

    # region business
    df = df.withColumn(
        "region",
        F.when(F.col("country").isin("FR", "ES", "IT"), "EUROPE")
        .when(F.col("country").isin("DE", "NL", "BE"), "BENELUX_DACH")
        .otherwise("OTHER"),
    )

    # flags KPI
    df = df.withColumn("is_high_value", F.col("amount_eur") > 300)
    df = df.withColumn("big_spender", F.col("amount_eur") > 200)

    # risk payment
    df = df.withColumn(
        "payment_risk",
        F.when(F.col("payment_method") == "cash", "low")
        .when(F.col("payment_method") == "card", "medium")
        .otherwise("high"),
    )

    # partition date
    df = df.withColumn("ds", F.lit(logical_date))

    return df


# =========================
# TRANSFORM 3 - KPI AGG
# =========================
def transform_3(df: DataFrame) -> DataFrame:

    kpis = df.groupBy("country", "category", "payment_method").agg(
        F.sum("amount_eur").alias("revenue"),
        F.count("*").alias("transactions"),
        F.avg("amount_eur").alias("avg_ticket"),
        F.max("amount_eur").alias("max_ticket"),
    )

    return kpis


# =========================
# PIPELINE RUN
# =========================
def run_daily(logical_date: str) -> dict:

    spark = SparkSession.builder.appName(f"lab4-kpis-{logical_date}").getOrCreate()

    # 1. READ + CLEAN
    df = transform_1(spark, logical_date)

    # 2. ENRICH
    df = transform_2(df, logical_date)

    # 3. AGGREGATE
    kpis = transform_3(df)

    # =========================
    # WRITE CURATED PARQUET
    # =========================
    curated_path = str(curated_kpis(logical_date))
    kpis.write.mode("overwrite").parquet(curated_path)

    # =========================
    # GLOBAL KPI (CLEAN FIX)
    # =========================
    global_kpi = df.agg(
        F.sum("amount_eur").alias("total_revenue"),
        F.count("*").alias("total_transactions"),
    ).collect()[0]

    # =========================
    # DASHBOARD JSON
    # =========================
    dashboard_path = str(report_json(logical_date))

    dashboard = {
        "logical_date": logical_date,
        "total_revenue": float(global_kpi["total_revenue"]),
        "total_transactions": int(global_kpi["total_transactions"]),
        "curated_path": curated_path,
    }

    import json
    import os

    os.makedirs(os.path.dirname(dashboard_path), exist_ok=True)

    with open(dashboard_path, "w") as f:
        json.dump(dashboard, f, indent=2)

    spark.stop()

    return dashboard
