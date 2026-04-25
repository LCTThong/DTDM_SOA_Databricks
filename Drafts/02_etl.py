# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — ETL Service
# MAGIC
# MAGIC **Kiến trúc SOA:** `[Ingestion Service] → [ETL Service]`
# MAGIC
# MAGIC Nhiệm vụ:
# MAGIC - Đọc staging data từ notebook 01
# MAGIC - Làm sạch: xử lý null, chuẩn hóa kiểu dữ liệu, loại bỏ duplicate
# MAGIC - Làm giàu: tách địa chỉ, tính tuổi, parse thể loại
# MAGIC - Tách thành bảng Dimension (DIM) và Fact (FACT)
# MAGIC - Xuất bảng sạch sang staging layer 2

# COMMAND ----------

# ============================================================
# PATH CONFIGURATION (UNITY CATALOG VOLUME)
# ============================================================

RAW_PATH     = "/Volumes/workspace/default/data_film_company/"
STAGING_PATH = "/Volumes/workspace/default/data_film_company/staging"
#CLEANED_PATH = "/Volumes/workspace/default/data_film_company/cleaned"
print("RAW_PATH    :", RAW_PATH)
print("STAGING_PATH:", STAGING_PATH)

# COMMAND ----------

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

from pyspark.sql import functions as F

def clean_columns(df):
    """
    Chuẩn hóa tên cột:
    - bỏ BOM
    - lowercase
    - space -> underscore
    """
    return df.toDF(
        *[c.strip().lstrip('\ufeff').lower().replace(" ", "_") for c in df.columns]
    )

def ingestion_report(df, name):
    total = df.count()
    print(f"\n{'='*50}")
    print(f"TABLE: {name}")
    print(f"{'='*50}")
    print(f"Rows    : {total:,}")
    print(f"Columns : {df.columns}")

# COMMAND ----------

# ============================================================
# FILM
# ============================================================

df_film_raw = (
    spark.read
         .option("header", True)
         .option("sep", ";")
         .option("encoding", "UTF-8")
         .option("multiLine", True)
         .option("quote", '"')
         .option("escape", '"')
         .csv(RAW_PATH + "Film.csv")
)

df_film_raw = clean_columns(df_film_raw)
ingestion_report(df_film_raw, "FILM RAW")

df_film_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .save(STAGING_PATH + "/film")

# COMMAND ----------

# ============================================================
# CUSTOMER
# ============================================================

df_customer_raw = (
    spark.read
         .option("header", True)
         .option("sep", ";")
         .option("encoding", "UTF-8")
         .csv(RAW_PATH + "Customer.csv")
)

df_customer_raw = clean_columns(df_customer_raw)
ingestion_report(df_customer_raw, "CUSTOMER RAW")

df_customer_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .save(STAGING_PATH + "/customer")

# COMMAND ----------

# ============================================================
# TICKET
# ============================================================

df_ticket_raw = (
    spark.read
         .option("header", True)
         .option("sep", ";")
         .option("encoding", "UTF-8")
         .csv(RAW_PATH + "Ticket.csv")
)

df_ticket_raw = clean_columns(df_ticket_raw)
ingestion_report(df_ticket_raw, "TICKET RAW")

df_ticket_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .save(STAGING_PATH + "/ticket")

# COMMAND ----------

display(dbutils.fs.ls(STAGING_PATH))
display(dbutils.fs.ls(STAGING_PATH + "/film"))
display(dbutils.fs.ls(STAGING_PATH + "/customer"))
display(dbutils.fs.ls(STAGING_PATH + "/ticket"))

# COMMAND ----------

# ============================================================
# READ DELTA TABLES
# ============================================================

df_film     = spark.read.format("delta").load(STAGING_PATH + "/film")
df_customer = spark.read.format("delta").load(STAGING_PATH + "/customer")
df_ticket   = spark.read.format("delta").load(STAGING_PATH + "/ticket")

print("film     :", df_film.count())
print("customer :", df_customer.count())
print("ticket   :", df_ticket.count())

display(df_film)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ============================================================
# ETL — DIM_CUSTOMER (CLEAN VERSION)
# ============================================================

dim_customer = (
    df_customer
    # 1. Rename
    .withColumnRenamed("customerid", "customer_id")

    # 2. Parse DOB
    .withColumn(
        "dob",
        F.to_date(F.col("dob"), "dd/MM/yyyy")
    )

    # 3. Age in 2019
    .withColumn(
        "age_2019",
        (2019 - F.year(F.col("dob"))).cast(IntegerType())
    )

    # 4. Age group
    .withColumn(
        "age_group",
        F.when(F.col("age_2019") < 20, "Dưới 20")
         .when(F.col("age_2019") < 30, "20-29")
         .when(F.col("age_2019") < 40, "30-39")
         .when(F.col("age_2019") < 50, "40-49")
         .otherwise("50+")
    )

    # 5. Address split
    .withColumn("street",   F.trim(F.split(F.col("address"), "/").getItem(0)))
    .withColumn("district", F.trim(F.split(F.col("address"), "/").getItem(1)))
    .withColumn("city",     F.trim(F.split(F.col("address"), "/").getItem(2)))

    # 6. Normalize gender
    .withColumn(
        "gender",
        F.when(F.upper(F.col("gender")).isin("M", "MALE"), "Male")
         .when(F.upper(F.col("gender")).isin("F", "FEMALE"), "Female")
         .otherwise("Other")
    )

    # 7. Customer type
    .withColumn(
        "customer_type",
        F.when(F.col("customer_id").startswith("WEBS"), "Online")
         .otherwise("Offline")
    )

    # 8. Remove bad rows
    .dropna(subset=["customer_id"])
    .dropDuplicates(["customer_id"])

    # 9. Final schema
    .select(
        "customer_id", "dob", "age_2019", "age_group",
        "gender", "district", "city", "job", "customer_type"
    )
)

print(f"DIM_CUSTOMER: {dim_customer.count():,} rows")
display(dim_customer.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

# ============================================================
# ETL — DIM_DATE (Spark 3.x compatible)
# ============================================================

date_range = (
    df_ticket
    .select(F.to_date(F.col("date")).alias("sale_date"))
    .filter(F.col("sale_date").isNotNull())
)

dim_date = (
    date_range
    .dropDuplicates(["sale_date"])

    .withColumn("day",   F.dayofmonth("sale_date"))
    .withColumn("month", F.month("sale_date"))
    .withColumn("year",  F.year("sale_date"))

    # Convert Spark dayofweek (1=Sun..7=Sat) → ISO (Mon=1..Sun=7)
    .withColumn(
        "day_of_week",
        F.when(F.dayofweek("sale_date") == 1, 7)
         .otherwise(F.dayofweek("sale_date") - 1)
    )

    .withColumn("day_name",   F.date_format("sale_date", "EEEE"))
    .withColumn("month_name", F.date_format("sale_date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("sale_date"))
    .withColumn("quarter", F.quarter("sale_date"))

    .withColumn(
        "is_weekend",
        F.col("day_of_week").isin(6, 7)
    )

    .orderBy("sale_date")
)

print(f"DIM_DATE: {dim_date.count():,} rows")
display(dim_date)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# ============================================================
# ETL — FACT_TICKET (CLEAN & STABLE)
# ============================================================

fact_ticket = (
    df_ticket
    # 1. Rename keys
    .withColumnRenamed("orderid",    "order_id")
    .withColumnRenamed("customerid", "customer_id")
    .withColumnRenamed("ticketcode", "ticket_id")
    .withColumnRenamed("film",       "film_name")

    # 2. Parse sale_date (safe)
    .withColumn("sale_date", F.to_date(F.col("date")))

    # 3. Sale datetime & hour
    .withColumn("sale_datetime",
        F.to_timestamp(F.col("saledate")))
    .withColumn("sale_hour",
        F.hour(F.col("sale_datetime")))

    # 4. Show hour
    .withColumn("show_hour",
        F.split(F.col("time"), ":").getItem(0).cast(IntegerType()))

    # 5. Show session
    .withColumn("show_session",
        F.when(F.col("show_hour") < 12, "Sáng")
         .when(F.col("show_hour") < 17, "Chiều")
         .when(F.col("show_hour") < 20, "Tối sớm")
         .otherwise("Tối muộn"))

    # 6. Cast numeric
    .withColumn("ticket_price", F.col("ticket_price").cast(DoubleType()))
    .withColumn("total",        F.col("total").cast(DoubleType()))
    .withColumn("room",         F.col("room").cast(IntegerType()))

    # 7. Normalize popcorn
    .withColumn("has_popcorn",
        F.when(F.trim(F.col("popcorn")) == "Có", True).otherwise(False))

    # 8. Normalize slot_type
    .withColumn("is_couple_seat",
        F.when(F.upper(F.col("slot_type")) == "ĐÔI", True).otherwise(False))

    # 9. Clean strings (important for join)
    .withColumn("film_name",  F.trim(F.col("film_name")))
    .withColumn("slot_type",  F.trim(F.col("slot_type")))
    .withColumn("ticket_type",F.trim(F.col("ticket_type")))
    .withColumn("cashier",    F.trim(F.col("cashier")))

    # 10. Remove bad rows
    .dropna(subset=["order_id", "customer_id", "film_name", "sale_date"])
    .filter(F.col("ticket_price") > 0)

    # 11. Weekend flag (Spark standard)
    .withColumn("is_weekend",
        F.dayofweek("sale_date").isin(1, 7))

    # 12. Final schema
    .select(
        "order_id", "ticket_id", "customer_id", "film_name",
        "sale_date", "is_weekend",
        "sale_hour", "time", "show_hour", "show_session",
        "slot", "room", "slot_type", "is_couple_seat",
        "ticket_type", "ticket_price", "total",
        "has_popcorn", "cashier"
    )
)

print(f"FACT_TICKET: {fact_ticket.count():,} rows")
display(fact_ticket.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

# ============================================================
# DATA QUALITY CHECK — FACT_TICKET
# ============================================================

print("=== FACT_TICKET QUALITY CHECK ===")

# Date range
fact_ticket.select(
    F.min("sale_date").alias("min_sale_date"),
    F.max("sale_date").alias("max_sale_date")
).show()

# Cardinality
print("Unique films   :", fact_ticket.select("film_name").distinct().count())
print("Unique customer:", fact_ticket.select("customer_id").distinct().count())

# Categorical distribution
print("\nSlot type distribution:")
fact_ticket.groupBy("slot_type").count().orderBy("count", ascending=False).show()

print("Show session distribution:")
fact_ticket.groupBy("show_session").count().orderBy("count", ascending=False).show()

# Numeric sanity
fact_ticket.select(
    F.sum(F.when(F.col("ticket_price") <= 0, 1).otherwise(0)).alias("invalid_ticket_price"),
    F.sum(F.when(F.col("total") <= 0, 1).otherwise(0)).alias("invalid_total")
).show()


# ============================================================
# DATA QUALITY CHECK — DIM_CUSTOMER
# ============================================================

print("=== DIM_CUSTOMER QUALITY CHECK ===")

print("Gender distribution:")
dim_customer.groupBy("gender").count().show()

print("Age group distribution:")
dim_customer.groupBy("age_group").count().orderBy("age_group").show()

print("Customer type distribution:")
dim_customer.groupBy("customer_type").count().show()

# COMMAND ----------

dim_film = spark.read.format("delta").load(
    CLEANED_PATH + "/dim_film"
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

dim_film = (
    df_film
    .withColumnRenamed("film", "film_name")

    # Clean text
    .withColumn("film_name", F.trim(F.col("film_name")))
    .withColumn("country",   F.trim(F.col("country")))
    .withColumn("director",  F.trim(F.col("director")))
    .withColumn("rating",    F.trim(F.col("rating")))

    # ✅ FIX duration (chặn chuỗi rỗng + extract số)
    .withColumn(
        "duration",
        F.when(
            F.col("duration").rlike(r"\d+"),
            F.regexp_extract(F.col("duration"), r"(\d+)", 1).cast(IntegerType())
        ).otherwise(None)
    )

    .dropna(subset=["film_name"])
    .dropDuplicates(["film_name"])

    .select(
        "film_name",
        "country",
        "director",
        "rating",
        "duration"
    )
)

print(f"DIM_FILM: {dim_film.count():,} rows")
display(dim_film)

# COMMAND ----------

required_dfs = ["dim_film", "dim_customer", "dim_date", "fact_ticket"]

for df in required_dfs:
    print(df, "=>", df in globals())

# COMMAND ----------

BASE_PATH = "/Volumes/workspace/default/data_film_company/cleaned"

dim_film.write.mode("overwrite").parquet(f"{BASE_PATH}/dim_film")
dim_customer.write.mode("overwrite").parquet(f"{BASE_PATH}/dim_customer")
dim_date.write.mode("overwrite").parquet(f"{BASE_PATH}/dim_date")
fact_ticket.write.mode("overwrite").parquet(f"{BASE_PATH}/fact_ticket")

print("✅ ĐÃ GHI XONG CLEANED DATA")