# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestion Service
# MAGIC
# MAGIC **Kiến trúc SOA:** `[Data Source] → [Ingestion Service]`
# MAGIC
# MAGIC Nhiệm vụ:
# MAGIC - Đọc 3 file CSV gốc từ DBFS
# MAGIC - Kiểm tra schema, số dòng, null values
# MAGIC - Lưu raw data vào Parquet staging area
# MAGIC
# MAGIC ---
# MAGIC > **Cách upload data:** Databricks → *Data* → *Add Data* → *Upload File*  
# MAGIC > Sau khi upload, file nằm tại `/FileStore/tables/`

# COMMAND ----------

# DBTITLE 1,Cell 2
# ============================================================
# CẤU HÌNH ĐƯỜNG DẪN
# ============================================================
# Unity Catalog Volume path (thay thế /FileStore/tables/ đã bị vô hiệu hóa)
# Upload CSV files vào: Data > Volumes > workspace.default.film_data > Upload Files

RAW_PATH     = "/Volumes/workspace/default/data_film_company/"
STAGING_PATH = "/Volumes/workspace/default/data_film_company/staging/"

print(f"Raw path    : {RAW_PATH}")
print(f"Staging path: {STAGING_PATH}")

# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

# COMMAND ----------

# ============================================================
# HÀM TIỆN ÍCH
# ============================================================
from pyspark.sql import functions as F

def ingestion_report(df, name):
    """In báo cáo chất lượng data cho 1 dataframe."""
    total = df.count()
    print(f"\n{'='*50}")
    print(f"  TABLE: {name}")
    print(f"{'='*50}")
    print(f"  Số dòng  : {total:,}")
    print(f"  Số cột   : {len(df.columns)}")
    print(f"  Cột      : {df.columns}")
    print()

    # Null count theo cột
    null_counts = df.select([
        F.count(F.when(F.col(c).isNull() | (F.col(c) == ""), c)).alias(c)
        for c in df.columns
    ])
    print("  Null / Empty counts:")
    null_row = null_counts.collect()[0].asDict()
    for col, cnt in null_row.items():
        flag = " ⚠️" if cnt > 0 else ""
        print(f"    {col:25s}: {cnt:,}{flag}")
    print()

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/data_film_company/")

# COMMAND ----------

# DBTITLE 1,Cell 4
# ============================================================
# ĐỌC CSV — FILM
# ============================================================
df_film_raw = (
    spark.read
         .option("header",      True)
         .option("sep",         ";")
         .option("encoding",    "UTF-8")
         .option("multiLine",   True)       # description có thể có xuống dòng
         .option("quote",       '"')
         .option("escape",      '"')
         .csv(RAW_PATH + "Film.csv")  # Ensure RAW_PATH is correctly defined and accessible
)

# Đổi tên cột bị lạ do BOM hoặc khoảng trắng
df_film_raw = df_film_raw.toDF(*[c.strip().lstrip('\ufeff') for c in df_film_raw.columns])

ingestion_report(df_film_raw, "FILM (raw)")
display(df_film_raw)

# COMMAND ----------

# ============================================================
# ĐỌC CSV — CUSTOMER
# ============================================================
df_customer_raw = (
    spark.read
         .option("header",   True)
         .option("sep",      ";")
         .option("encoding", "UTF-8")
         .csv(RAW_PATH + "Customer.csv")
)

df_customer_raw = df_customer_raw.toDF(*[c.strip().lstrip('\ufeff') for c in df_customer_raw.columns])

ingestion_report(df_customer_raw, "CUSTOMER (raw)")
display(df_customer_raw.limit(10))

# COMMAND ----------

# ============================================================
# ĐỌC CSV — TICKET
# ============================================================
df_ticket_raw = (
    spark.read
         .option("header",   True)
         .option("sep",      ";")
         .option("encoding", "UTF-8")
         .csv(RAW_PATH + "Ticket.csv")
)

df_ticket_raw = df_ticket_raw.toDF(*[c.strip().lstrip('\ufeff') for c in df_ticket_raw.columns])

ingestion_report(df_ticket_raw, "TICKET (raw)")
display(df_ticket_raw.limit(10))

# COMMAND ----------

# ============================================================
# GHI STAGING (Parquet — nhanh hơn CSV cho bước ETL)
# ============================================================
df_film_raw.write.mode("overwrite").parquet(STAGING_PATH + "/film")
df_customer_raw.write.mode("overwrite").parquet(STAGING_PATH + "/customer")
df_ticket_raw.write.mode("overwrite").parquet(STAGING_PATH + "/ticket")

print("✅ Ingestion Service hoàn thành!")
print(f"   Staging data tại: {STAGING_PATH}")
print()
print("   Film     :", df_film_raw.count(), "rows")
print("   Customer :", df_customer_raw.count(), "rows")
print("   Ticket   :", df_ticket_raw.count(), "rows")