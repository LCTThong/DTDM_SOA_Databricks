# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Analytics Service
# MAGIC
# MAGIC **Kiến trúc SOA:** `[Storage Service] → [Analytics Service]`
# MAGIC
# MAGIC Nhiệm vụ:
# MAGIC - Đọc Delta Lake tables
# MAGIC - Tính 6 nhóm KPI
# MAGIC - Vẽ biểu đồ phân tích
# MAGIC
# MAGIC | # | KPI Group | Mô tả |
# MAGIC |---|-----------|-------|
# MAGIC | 1 | Tổng quan | Summary toàn bộ dataset |
# MAGIC | 2 | Doanh thu | Theo ngày, phim |
# MAGIC | 3 | Phim | Top phim, thể loại, rating |
# MAGIC | 4 | Khách hàng | Giới tính, tuổi, khu vực |
# MAGIC | 5 | Vé & Suất chiếu | Loại vé, khung giờ, phòng |
# MAGIC | 6 | Bắp rang | Tương quan doanh thu |
# MAGIC

# COMMAND ----------

# ============================================================
# SETUP — LOAD FROM UNITY CATALOG
# ============================================================

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

DB_NAME = "workspace.default"

spark.sql(f"USE {DB_NAME}")

# Load Delta tables (UC standard)
dim_film     = spark.table(f"{DB_NAME}.dim_film")
dim_customer = spark.table(f"{DB_NAME}.dim_customer")
fact_ticket  = spark.table(f"{DB_NAME}.fact_ticket")

print("✅ Load từ Delta Lake thành công")
print(f"   fact_ticket  : {fact_ticket.count():,} rows")
print(f"   dim_film     : {dim_film.count():,} rows")
print(f"   dim_customer : {dim_customer.count():,} rows")

# COMMAND ----------

# ============================================================
# KPI 1 — SUMMARY TỔNG QUAN
# ============================================================

summary = spark.sql(f"""
    SELECT
        COUNT(DISTINCT order_id)     AS tong_don_hang,
        COUNT(ticket_id)             AS tong_ve_ban,
        COUNT(DISTINCT customer_id)  AS tong_khach_hang,
        COUNT(DISTINCT film_name)    AS tong_phim,
        ROUND(SUM(total), 0)         AS tong_doanh_thu_vnd,
        ROUND(AVG(ticket_price), 0)  AS gia_ve_tb_vnd,
        MIN(sale_date)               AS ngay_dau,
        MAX(sale_date)               AS ngay_cuoi
    FROM {DB_NAME}.fact_ticket
""")

print("=" * 55)
print("  SUMMARY — HỆ THỐNG RẠP PHIM THÁNG 5/2019")
print("=" * 55)

row = summary.first()

if row:
    print(f"  Tổng đơn hàng  : {row.tong_don_hang:>10,}")
    print(f"  Tổng vé bán    : {row.tong_ve_ban:>10,}")
    print(f"  Tổng khách     : {row.tong_khach_hang:>10,}")
    print(f"  Số phim chiếu  : {row.tong_phim:>10}")
    print(f"  Doanh thu      : {row.tong_doanh_thu_vnd:>10,.0f} VNĐ")
    print(f"  Giá vé TB      : {row.gia_ve_tb_vnd:>10,.0f} VNĐ")
    print(f"  Khoảng thời gian: {row.ngay_dau} → {row.ngay_cuoi}")
else:
    print("⚠ Không có dữ liệu")

# COMMAND ----------

# ============================================================
# KPI 2A — DOANH THU THEO NGÀY
# ============================================================

revenue_daily = spark.sql(f"""
    SELECT
        sale_date,
        COUNT(ticket_id)             AS so_ve,
        COUNT(DISTINCT order_id)     AS so_don,
        ROUND(SUM(total), 0)         AS doanh_thu
    FROM {DB_NAME}.fact_ticket
    GROUP BY sale_date
    ORDER BY sale_date
""")

display(revenue_daily)

# COMMAND ----------

revenue_daily = spark.sql(f"""
WITH daily AS (
    SELECT
        sale_date,
        COUNT(ticket_id) AS so_ve,
        ROUND(SUM(total), 0) AS doanh_thu
    FROM {DB_NAME}.fact_ticket
    GROUP BY sale_date
)

SELECT *,
       LAG(doanh_thu) OVER (ORDER BY sale_date) AS doanh_thu_ngay_truoc,
       ROUND(doanh_thu - LAG(doanh_thu) OVER (ORDER BY sale_date), 0) AS tang_giam
FROM daily
ORDER BY sale_date
""")

# COMMAND ----------

# ============================================================
# BIỂU ĐỒ DOANH THU THEO NGÀY
# ============================================================

pdf_daily = revenue_daily.toPandas()

# đảm bảo sort đúng
pdf_daily = pdf_daily.sort_values("sale_date")

fig, ax1 = plt.subplots(figsize=(14, 5))

# chỉ vẽ bar doanh thu
bars = ax1.bar(
    pdf_daily['sale_date'].astype(str),
    pdf_daily['doanh_thu'] / 1e6,
    color='#3498db',
    edgecolor='white',
    linewidth=0.5
)

ax1.set_xlabel('Ngày', fontsize=11)
ax1.set_ylabel('Doanh thu (triệu VNĐ)', fontsize=11)
ax1.set_title(
    'Doanh thu theo ngày — Tháng 5/2019',
    fontsize=13,
    fontweight='bold'
)

ax1.tick_params(axis='x', rotation=60)

# format Y axis
ax1.yaxis.set_major_formatter(
    mticker.FuncFormatter(lambda x, _: f'{x:.1f}M')
)

# line chart số vé
ax2 = ax1.twinx()
ax2.plot(
    pdf_daily['sale_date'].astype(str),
    pdf_daily['so_ve'],
    color='#2ecc71',
    marker='o',
    linewidth=2,
    markersize=4
)

ax2.set_ylabel('Số vé bán', fontsize=11, color='#2ecc71')
ax2.tick_params(axis='y', colors='#2ecc71')

plt.tight_layout()
plt.show()

print("Biểu đồ doanh thu theo ngày ✅")

# COMMAND ----------

pdf_daily['doanh_thu_ma7'] = pdf_daily['doanh_thu'].rolling(7).mean() / 1e6

ax1.plot(
    pdf_daily['sale_date'].astype(str),
    pdf_daily['doanh_thu_ma7'],
    color='orange',
    linewidth=2,
    label='MA7 doanh thu'
)

ax1.legend()

# COMMAND ----------

# ============================================================
# KPI 2B — SO SÁNH NGÀY THƯỜNG VS CUỐI TUẦN
# ============================================================

weekend_compare = spark.sql(f"""
    SELECT
        CASE WHEN is_weekend THEN 'Cuối tuần' ELSE 'Ngày thường' END AS loai_ngay,
        COUNT(ticket_id)                          AS tong_ve,
        COUNT(DISTINCT order_id)                 AS tong_don,
        ROUND(SUM(total), 0)                     AS tong_doanh_thu,
        ROUND(AVG(ticket_price), 0)              AS gia_ve_tb,
        ROUND(
            SUM(total) / COUNT(DISTINCT sale_date),
            0
        ) AS dt_tb_moi_ngay
    FROM {DB_NAME}.fact_ticket
    GROUP BY is_weekend
""")

print("=== Ngày thường vs Cuối tuần ===")
display(weekend_compare)

# COMMAND ----------

weekend_compare = spark.sql(f"""
WITH base AS (
    SELECT
        CASE WHEN is_weekend THEN 'Cuối tuần' ELSE 'Ngày thường' END AS loai_ngay,
        COUNT(ticket_id) AS tong_ve,
        ROUND(SUM(total), 0) AS tong_doanh_thu
    FROM {DB_NAME}.fact_ticket
    GROUP BY is_weekend
)

SELECT *,
       ROUND(
           tong_doanh_thu / SUM(tong_doanh_thu) OVER(),
           2
       ) AS ty_le_doanh_thu
FROM base
""")

# COMMAND ----------

top_films = spark.sql(f"""
SELECT
    t.film_name,
    f.rating,
    f.duration,
    COUNT(t.ticket_id) AS so_ve,
    ROUND(SUM(t.total), 0) AS doanh_thu,
    ROUND(AVG(t.ticket_price), 0) AS gia_tb
FROM {DB_NAME}.fact_ticket t
LEFT JOIN {DB_NAME}.dim_film f
    ON TRIM(t.film_name) = TRIM(f.film_name)
GROUP BY t.film_name, f.rating, f.duration
ORDER BY doanh_thu DESC
LIMIT 10
""")

# COMMAND ----------

top_films = spark.sql(f"""
SELECT
    t.film_name,
    f.rating,
    f.duration,
    COUNT(t.ticket_id) AS so_ve,
    ROUND(SUM(t.total), 2) AS doanh_thu,
    ROUND(
        SUM(t.total) / COUNT(t.ticket_id),
        2
    ) AS gia_tb
FROM {DB_NAME}.fact_ticket t
LEFT JOIN {DB_NAME}.dim_film f
    ON TRIM(t.film_name) = TRIM(f.film_name)
GROUP BY
    t.film_name,
    f.rating,
    f.duration
ORDER BY doanh_thu DESC
LIMIT 10
""")

# COMMAND ----------

import matplotlib.pyplot as plt

# convert sang pandas
pdf = top_films.toPandas()

# sort lại cho đẹp
pdf = pdf.sort_values("doanh_thu", ascending=True)

plt.figure(figsize=(12,6))

plt.barh(
    pdf["film_name"],
    pdf["doanh_thu"] / 1e6
)

plt.xlabel("Doanh thu (triệu VNĐ)")
plt.ylabel("Phim")
plt.title("Top 10 phim doanh thu cao nhất")
plt.tight_layout()

plt.show()

# COMMAND ----------

genre_analysis = spark.sql(f"""
WITH base AS (
    SELECT
        COALESCE(f.rating, 'Unknown') AS genre,
        COUNT(t.ticket_id) AS so_ve,
        SUM(t.total) AS doanh_thu
    FROM {DB_NAME}.fact_ticket t
    LEFT JOIN {DB_NAME}.dim_film f
        ON TRIM(t.film_name) = TRIM(f.film_name)
    GROUP BY COALESCE(f.rating, 'Unknown')
)

SELECT
    genre,
    so_ve,
    ROUND(doanh_thu, 0) AS doanh_thu,
    ROUND(so_ve * 100.0 / SUM(so_ve) OVER(), 2) AS pct_ve
FROM base
ORDER BY so_ve DESC
""")

print("=== Phân bố thể loại ===")
display(genre_analysis)

# COMMAND ----------

import matplotlib.pyplot as plt

pdf_genre = genre_analysis.toPandas()

# sort theo số vé
pdf_genre = pdf_genre.sort_values("so_ve", ascending=False)

# (optional) bỏ Unknown nếu muốn sạch report
pdf_genre = pdf_genre[pdf_genre["genre"] != "Unknown"]

fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# =========================
# PIE CHART
# =========================
axes[0].pie(
    pdf_genre['so_ve'],
    labels=pdf_genre['genre'],
    autopct='%1.1f%%',
    startangle=140
)
axes[0].set_title(
    'Tỷ lệ số vé theo thể loại',
    fontsize=12,
    fontweight='bold'
)

# =========================
# BAR CHART
# =========================
axes[1].bar(
    pdf_genre['genre'],
    pdf_genre['doanh_thu'] / 1e6
)

axes[1].set_xlabel('Thể loại', fontsize=10)
axes[1].set_ylabel('Doanh thu (triệu VNĐ)', fontsize=10)
axes[1].set_title(
    'Doanh thu theo thể loại',
    fontsize=12,
    fontweight='bold'
)

axes[1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

print("Biểu đồ thể loại ✅")

# COMMAND ----------

customer_gender = spark.sql(f"""
    SELECT
        c.gender,
        COUNT(DISTINCT t.customer_id) AS so_khach,
        COUNT(t.ticket_id)            AS so_ve,
        ROUND(SUM(t.total), 0)        AS tong_chi_tieu,
        ROUND(
            SUM(t.total) / COUNT(t.ticket_id),
            0
        ) AS chi_tieu_tb_moi_ve
    FROM {DB_NAME}.fact_ticket t
    LEFT JOIN {DB_NAME}.dim_customer c
        ON t.customer_id = c.customer_id
    GROUP BY c.gender
    ORDER BY so_ve DESC
""")

print("=== Phân tích theo giới tính ===")
display(customer_gender)

# COMMAND ----------

customer_age = spark.sql(f"""
    SELECT
        c.age_group,
        c.gender,
        COUNT(DISTINCT t.customer_id) AS so_khach,
        COUNT(t.ticket_id)            AS so_ve,
        ROUND(SUM(t.total), 0)        AS tong_chi_tieu,
        ROUND(
            SUM(t.total) / COUNT(t.ticket_id),
            0
        ) AS chi_tieu_tb_moi_ve
    FROM {DB_NAME}.fact_ticket t
    LEFT JOIN {DB_NAME}.dim_customer c
        ON t.customer_id = c.customer_id
    GROUP BY c.age_group, c.gender
    ORDER BY
        CASE c.age_group
            WHEN 'Dưới 20' THEN 1
            WHEN '20-29'   THEN 2
            WHEN '30-39'   THEN 3
            WHEN '40-49'   THEN 4
            ELSE 5
        END,
        c.gender
""")

print("=== Phân tích theo nhóm tuổi & giới tính ===")
display(customer_age)

# COMMAND ----------

pdf_age = customer_age.toPandas()

# pivot
pdf_pivot = pdf_age.pivot(
    index='age_group',
    columns='gender',
    values='so_ve'
).fillna(0)

# đảm bảo thứ tự nhóm tuổi
age_order = ['Dưới 20', '20-29', '30-39', '40-49', '50+']
pdf_pivot = pdf_pivot.reindex(age_order).fillna(0)

# đảm bảo luôn có đủ cột gender
for col in pdf_pivot.columns:
    pdf_pivot[col] = pdf_pivot[col].fillna(0)

# vẽ chart
ax = pdf_pivot.plot(
    kind='bar',
    figsize=(10, 5),
    edgecolor='white',
    width=0.7
)

ax.set_xlabel('Nhóm tuổi', fontsize=11)
ax.set_ylabel('Số vé', fontsize=11)
ax.set_title(
    'Số vé theo nhóm tuổi & giới tính',
    fontsize=13,
    fontweight='bold'
)

ax.tick_params(axis='x', rotation=0)
ax.legend(title='Giới tính')

plt.tight_layout()
plt.show()

print("Biểu đồ nhóm tuổi ✅")

# COMMAND ----------

customer_district = spark.sql(f"""
    SELECT
        c.district,
        c.city,
        COUNT(DISTINCT t.customer_id) AS so_khach,
        COUNT(t.ticket_id)            AS so_ve,
        ROUND(SUM(t.total), 0)        AS doanh_thu,
        ROUND(SUM(t.total) / COUNT(DISTINCT t.customer_id), 0) AS chi_tieu_tb_khach
    FROM {DB_NAME}.fact_ticket t
    LEFT JOIN {DB_NAME}.dim_customer c
        ON t.customer_id = c.customer_id
    GROUP BY c.district, c.city
    ORDER BY doanh_thu DESC
    LIMIT 15
""")

print("=== Top 15 Quận/Huyện ===")
display(customer_district)

# COMMAND ----------

session_analysis = spark.sql(f"""
    SELECT
        show_session,
        show_hour,
        COUNT(ticket_id)             AS so_ve,
        ROUND(SUM(total), 0)         AS doanh_thu,
        ROUND(
            COUNT(ticket_id) * 100.0 / SUM(COUNT(ticket_id)) OVER(),
        2) AS pct_ve
    FROM {DB_NAME}.fact_ticket
    GROUP BY show_session, show_hour
    ORDER BY show_hour
""")

print("=== Phân bố khung giờ chiếu ===")
display(session_analysis)

# COMMAND ----------

pdf_session = session_analysis.toPandas()

# sort theo giờ
pdf_session = pdf_session.sort_values("show_hour")

session_colors = {
    'Sáng': '#f39c12',
    'Chiều': '#3498db',
    'Tối sớm': '#9b59b6',
    'Tối muộn': '#2c3e50'
}

bar_colors = pdf_session['show_session'].map(session_colors).fillna('#95a5a6')

fig, ax = plt.subplots(figsize=(12, 5))

ax.bar(
    pdf_session['show_hour'].astype(str) + 'h',
    pdf_session['so_ve'],
    color=bar_colors,
    edgecolor='white'
)

# legend
from matplotlib.patches import Patch
legend = [Patch(facecolor=c, label=s) for s, c in session_colors.items()]
ax.legend(handles=legend, loc='upper left')

ax.set_xlabel('Giờ chiếu')
ax.set_ylabel('Số vé')
ax.set_title('Phân bố số vé theo giờ chiếu')

plt.tight_layout()
plt.show()

print("OK")

# COMMAND ----------

room_analysis = spark.sql(f"""
    SELECT
        room,
        slot_type,
        COUNT(ticket_id)             AS so_ve,
        ROUND(SUM(total), 0)         AS doanh_thu,
        ROUND(AVG(total), 0)         AS gia_tb
    FROM {DB_NAME}.fact_ticket
    GROUP BY room, slot_type
    ORDER BY room, doanh_thu DESC
""")

print("=== Phân bố phòng chiếu & loại slot ===")
display(room_analysis)

# COMMAND ----------

room_analysis = spark.sql(f"""
    SELECT
        room,
        slot_type,
        COUNT(ticket_id)             AS so_ve,
        ROUND(SUM(total), 0)         AS doanh_thu,
        ROUND(AVG(total), 0)         AS gia_tb,

        -- KPI nâng cao
        ROUND(
            SUM(total) * 100.0 / SUM(SUM(total)) OVER(),
            2
        ) AS pct_revenue,

        ROUND(
            SUM(total) / COUNT(ticket_id),
            0
        ) AS revenue_per_ticket
    FROM {DB_NAME}.fact_ticket
    GROUP BY room, slot_type
    ORDER BY doanh_thu DESC
""")

print("=== Phân bố phòng chiếu & loại slot ===")
display(room_analysis)

# COMMAND ----------

slot_type_kpi = spark.sql(f"""
    SELECT
        slot_type,
        COUNT(ticket_id)                 AS so_ve,
        ROUND(SUM(total), 0)             AS doanh_thu,
        ROUND(AVG(total), 0)             AS gia_tb,

        -- KPI nâng cao
        ROUND(
            COUNT(ticket_id) * 100.0 / SUM(COUNT(ticket_id)) OVER(),
        2) AS pct_ve,

        ROUND(
            SUM(total) * 100.0 / SUM(SUM(total)) OVER(),
        2) AS pct_revenue,

        ROUND(
            SUM(total) / COUNT(ticket_id),
            0
        ) AS revenue_per_ticket

    FROM {DB_NAME}.fact_ticket
    GROUP BY slot_type
    ORDER BY so_ve DESC
""")

print("=== ĐÔI vs ĐƠN ===")
display(slot_type_kpi)

# COMMAND ----------

popcorn_analysis = spark.sql(f"""
    SELECT
        CASE WHEN has_popcorn THEN 'Có bắp rang' ELSE 'Không bắp rang' END AS trang_thai,

        COUNT(ticket_id)               AS so_ve,
        ROUND(SUM(total), 0)           AS doanh_thu,
        ROUND(AVG(total), 0)           AS gia_ve_tb,

        -- KPI nâng cao
        ROUND(
            COUNT(ticket_id) * 100.0 / SUM(COUNT(ticket_id)) OVER(),
        2) AS pct_ve,

        ROUND(
            SUM(total) * 100.0 / SUM(SUM(total)) OVER(),
        2) AS pct_revenue,

        ROUND(
            SUM(total) / COUNT(ticket_id),
            0
        ) AS revenue_per_ticket

    FROM {DB_NAME}.fact_ticket
    GROUP BY has_popcorn
    ORDER BY so_ve DESC
""")

print("=== Tỷ lệ mua bắp rang ===")
display(popcorn_analysis)

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle(
    'DASHBOARD — Phân tích Rạp phim Tháng 5/2019',
    fontsize=15,
    fontweight='bold'
)

# =========================
# 1. Top 5 phim
# =========================
pdf_top5 = top_films.toPandas().sort_values('doanh_thu', ascending=False).head(5)
pdf_top5['film_short'] = pdf_top5['film_name'].str[:20]

axes[0,0].barh(
    pdf_top5['film_short'][::-1],
    pdf_top5['doanh_thu'][::-1] / 1e6,
    color='#3498db'
)
axes[0,0].set_title('Top 5 Phim (Triệu VNĐ)', fontweight='bold')

# =========================
# 2. Doanh thu theo ngày
# =========================
pdf_daily_sorted = pdf_daily.sort_values('sale_date')

axes[0,1].plot(
    pdf_daily_sorted['sale_date'].astype(str),
    pdf_daily_sorted['doanh_thu'] / 1e6,
    color='#e74c3c',
    marker='o',
    linewidth=2
)
axes[0,1].set_title('Doanh thu theo ngày (Triệu VNĐ)', fontweight='bold')
axes[0,1].tick_params(axis='x', rotation=60)

# =========================
# 3. Thể loại
# =========================
pdf_g = genre_analysis.toPandas()

axes[1,0].pie(
    pdf_g['so_ve'],
    labels=pdf_g['genre'],
    autopct='%1.1f%%',
    startangle=90,
    colors=plt.cm.Set3.colors
)
axes[1,0].set_title('Tỷ lệ vé theo thể loại', fontweight='bold')

# =========================
# 4. Khung giờ
# =========================
pdf_s = session_analysis.toPandas()

pdf_s = pdf_s.groupby('show_session')['so_ve'].sum().reset_index()

session_order = ['Sáng', 'Chiều', 'Tối sớm', 'Tối muộn']
pdf_s = pdf_s.set_index('show_session').reindex(session_order).reset_index()

axes[1,1].bar(
    pdf_s['show_session'],
    pdf_s['so_ve'],
    color=['#f39c12','#3498db','#9b59b6','#2c3e50']
)

axes[1,1].set_title('Số vé theo khung giờ', fontweight='bold')

plt.tight_layout()
plt.show()

print("Dashboard tổng hợp OK")

# COMMAND ----------

# ============================================================
# XUẤT KẾT QUẢ ANALYTICS → DELTA LAKE (FIXED)
# ============================================================

DELTA_PATH = "/Volumes/workspace/default/data_film_company/delta"

# =========================
# 1. Revenue daily
# =========================
(
    revenue_daily.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{DELTA_PATH}/analytics_revenue_daily")
)

# =========================
# 2. Top films
# =========================
(
    top_films.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{DELTA_PATH}/analytics_top_films")
)

# =========================
# 3. Customer age
# =========================
(
    customer_age.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{DELTA_PATH}/analytics_customer_age")
)

print("✅ Analytics Service hoàn thành!")
print("   Delta tables đã lưu tại Volume")