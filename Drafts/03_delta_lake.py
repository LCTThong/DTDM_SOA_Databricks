# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Storage Service (Delta Lake)
# MAGIC
# MAGIC **Kiến trúc SOA:** `[ETL Service] → [Storage Service]`
# MAGIC
# MAGIC Nhiệm vụ:
# MAGIC - Đọc cleaned data từ notebook 02
# MAGIC - Ghi vào Delta Lake với ACID transactions
# MAGIC - Tạo database `film_db` và đăng ký bảng vào Metastore
# MAGIC - Kiểm tra Delta history & optimize

# COMMAND ----------

# ============================================================
# CẤU HÌNH CHUẨN — UNITY CATALOG
# ============================================================

CATALOG = "workspace"
SCHEMA  = "default"

BASE_VOLUME = "/Volumes/workspace/default/data_film_company"

CLEANED_PATH = f"{BASE_VOLUME}/cleaned"
DELTA_PATH   = f"{BASE_VOLUME}/delta"
DB_NAME      = f"{CATALOG}.{SCHEMA}"

print(f"Cleaned path : {CLEANED_PATH}")
print(f"Delta path   : {DELTA_PATH}")
print(f"Database     : {DB_NAME}")

# COMMAND ----------

# ============================================================
# ĐỌC CLEANED DATA — SAFE VERSION
# ============================================================
from pyspark.sql import functions as F

def read_parquet_safe(path, name):
    try:
        df = spark.read.parquet(path)
        print(f"✅ {name:<12}: {df.count():>7,} rows")
        return df
    except Exception as e:
        print(f"❌ {name:<12}: FAILED to load")
        raise e

dim_film     = read_parquet_safe(f"{CLEANED_PATH}/dim_film",     "dim_film")
dim_customer = read_parquet_safe(f"{CLEANED_PATH}/dim_customer", "dim_customer")
dim_date     = read_parquet_safe(f"{CLEANED_PATH}/dim_date",     "dim_date")
fact_ticket  = read_parquet_safe(f"{CLEANED_PATH}/fact_ticket",  "fact_ticket")

# COMMAND ----------

print("\n=== SCHEMA CHECK ===")
dim_film.printSchema()
dim_customer.printSchema()
dim_date.printSchema()
fact_ticket.printSchema()

# COMMAND ----------

# ============================================================
# TẠO SCHEMA
# ============================================================
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB_NAME}")
spark.sql(f"USE {DB_NAME}")

print(f"✅ Schema '{DB_NAME}' sẵn sàng")

# COMMAND ----------

# ============================================================
# LOAD DIM_FILM
# ============================================================
dim_film = spark.read.parquet(
    "dbfs:/Volumes/workspace/default/data_film_company/cleaned/dim_film"
)

print("dim_film loaded:", dim_film.count(), "rows")
display(dim_film.limit(5))

# COMMAND ----------

# ============================================================
# CONFIG — UNITY CATALOG
# ============================================================

CATALOG = "workspace"
SCHEMA  = "default"

DB_NAME = f"{CATALOG}.{SCHEMA}"

print(f"Database: {DB_NAME}")


# ============================================================
# USE DATABASE
# ============================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB_NAME}")
spark.sql(f"USE {DB_NAME}")


# ============================================================
# WRITE DIM_FILM (MANAGED TABLE - UC SAFE)
# ============================================================

dim_film.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{DB_NAME}.dim_film")

print("✅ dim_film saved:", spark.table(f"{DB_NAME}.dim_film").count(), "rows")


# ============================================================
# WRITE DIM_CUSTOMER
# ============================================================

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{DB_NAME}.dim_customer")

print("✅ dim_customer saved:", spark.table(f"{DB_NAME}.dim_customer").count(), "rows")


# ============================================================
# WRITE DIM_DATE
# ============================================================

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{DB_NAME}.dim_date")

print("✅ dim_date saved:", spark.table(f"{DB_NAME}.dim_date").count(), "rows")


# ============================================================
# WRITE FACT_TICKET
# ============================================================

fact_ticket.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{DB_NAME}.fact_ticket")

print("✅ fact_ticket saved:", spark.table(f"{DB_NAME}.fact_ticket").count(), "rows")


# ============================================================
# FINAL CHECK — TABLE LIST
# ============================================================

spark.sql(f"SHOW TABLES IN {DB_NAME}").show()

# COMMAND ----------

# ============================================================
# GHI DIM_CUSTOMER — UNITY CATALOG (FIXED)
# ============================================================

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{DB_NAME}.dim_customer")

print("✅ dim_customer ghi xong:",
      spark.table(f"{DB_NAME}.dim_customer").count(),
      "rows")

# COMMAND ----------

# ============================================================
# GHI DIM_DATE — UNITY CATALOG (FIXED)
# ============================================================

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{DB_NAME}.dim_date")

print("✅ dim_date ghi xong:",
      spark.table(f"{DB_NAME}.dim_date").count(),
      "rows")

# COMMAND ----------

from pyspark.sql import functions as F

fact_ticket = fact_ticket \
    .withColumn("sale_year", F.year("sale_date")) \
    .withColumn("sale_month", F.month("sale_date"))

# COMMAND ----------

fact_ticket.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("sale_year", "sale_month") \
    .saveAsTable(f"{DB_NAME}.fact_ticket")

print("✅ fact_ticket ghi xong:",
      spark.table(f"{DB_NAME}.fact_ticket").count(),
      "rows")

# COMMAND ----------

# ============================================================
# GHI FACT_TICKET — UNITY CATALOG (FIXED)
# ============================================================

fact_ticket.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("sale_year", "sale_month") \
    .saveAsTable(f"{DB_NAME}.fact_ticket")

print("✅ fact_ticket ghi xong:",
      spark.table(f"{DB_NAME}.fact_ticket").count(),
      "rows")

# COMMAND ----------

# ============================================================
# KIỂM TRA — SHOW TABLES (UC CLEAN VERSION)
# ============================================================

print(f"=== Tables trong database '{DB_NAME}' ===")

tables_df = spark.sql(f"SHOW TABLES IN {DB_NAME}") \
    .orderBy("tableName")

display(tables_df)

# COMMAND ----------

from pyspark.sql.functions import col

tables_df = spark.sql(f"SHOW TABLES IN {DB_NAME}") \
    .orderBy("tableName")

print("=== TABLE OVERVIEW ===")
display(tables_df)

# COMMAND ----------

print("=== Delta Transaction Log — fact_ticket ===")

history_df = spark.sql(f"DESCRIBE HISTORY {DB_NAME}.fact_ticket") \
    .orderBy("version", ascending=False)

display(history_df)

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {DB_NAME}.fact_ticket")
    .orderBy("version", ascending=False)
    .limit(10)
)

# COMMAND ----------

print("\n=== DELTA HISTORY (LATEST FIRST) ===\n")

display(
    spark.sql(f"DESCRIBE HISTORY {DB_NAME}.fact_ticket")
    .orderBy("version", ascending=False)
)

# COMMAND ----------

print("=== SCHEMA FACT_TICKET ===\n")

spark.table(f"{DB_NAME}.fact_ticket").printSchema()

# COMMAND ----------

# ============================================================
# OPTIMIZE — FACT TABLE (UNITY CATALOG SAFE)
# ============================================================

print("=== OPTIMIZE FACT TABLE ===")

try:
    spark.sql(f"OPTIMIZE {DB_NAME}.fact_ticket")

    print("✅ OPTIMIZE hoàn thành cho fact_ticket")

except Exception as e:
    print(f"⚠️ OPTIMIZE không khả dụng trên cluster này: {e}")

print("\n✅ Pipeline hoàn tất")
print(f"📌 Database: {DB_NAME}")

# COMMAND ----------

print("=== Demo Delta Time Travel (SQL) ===")

df_v0 = spark.sql(f"""
    SELECT * 
    FROM {DB_NAME}.fact_ticket VERSION AS OF 0
""")

print(f"Version 0 — fact_ticket: {df_v0.count():,} rows")