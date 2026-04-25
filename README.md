# Hệ thống Phân tích Dữ liệu Rạp Phim — SOA trên Databricks

> Đồ án giữa kỳ học phần **Kiến trúc hướng dịch vụ và Điện toán đám mây**
> Nền tảng: **Databricks** (Unity Catalog)

🔗 **GitHub:** `https://github.com/LCTThong/DTDM_SOA_Databricks.git`

---

## Mục lục

- [Giới thiệu](#giới-thiệu)
- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Cấu trúc dự án](#cấu-trúc-dự-án)
- [Dữ liệu đầu vào](#dữ-liệu-đầu-vào)
- [Hướng dẫn triển khai](#hướng-dẫn-triển-khai)
- [Chi tiết từng service](#chi-tiết-từng-service)
- [Mô hình dữ liệu](#mô-hình-dữ-liệu)
- [Kết quả phân tích](#kết-quả-phân-tích)
- [Thành viên nhóm](#thành-viên-nhóm)

---

## Giới thiệu

Dự án xây dựng hệ thống xử lý và phân tích dữ liệu bán vé rạp phim theo mô hình **SOA (Service-Oriented Architecture)** gồm 4 service độc lập, được triển khai hoàn toàn trên **Databricks** với Unity Catalog.

**Bài toán:** Từ dữ liệu giao dịch thô (CSV), xây dựng pipeline tự động hóa để:
- Làm sạch và chuẩn hóa dữ liệu
- Lưu trữ theo mô hình Star Schema trên Delta Lake
- Phân tích KPI: doanh thu, hành vi khách hàng, hiệu suất phòng chiếu, xu hướng theo thời gian

**Phạm vi dữ liệu:** Tháng 5/2019 — 35,472 giao dịch mua vé, 4,479 khách hàng, 19 bộ phim

---

## Kiến trúc hệ thống

```
┌──────────────────────────────────────────────────────────────────┐
│                       DATABRICKS PLATFORM                        │
│                    (Unity Catalog Workspace)                      │
│                                                                   │
│   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐           │
│   │   [01]      │   │   [02]      │   │   [03]      │           │
│   │ INGESTION   │──▶│    ETL      │──▶│  STORAGE    │           │
│   │  SERVICE    │   │  SERVICE    │   │  SERVICE    │           │
│   │             │   │             │   │ (Delta Lake)│           │
│   │ CSV → Raw   │   │ Raw → Clean │   │ Clean → UC  │           │
│   │ + Validate  │   │ DIM + FACT  │   │   Tables    │           │
│   └─────────────┘   └─────────────┘   └──────┬──────┘           │
│                                               │                   │
│                                        ┌──────▼──────┐           │
│                                        │   [04]      │           │
│                                        │ ANALYTICS   │           │
│                                        │  SERVICE    │           │
│                                        │             │           │
│                                        │ KPI + Charts│           │
│                                        └─────────────┘           │
│                                                                   │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │            DATABRICKS JOB — "Job for film data"           │  │
│   │   Task: run_film_analytics  |  Compute: Serverless        │  │
│   │   Retry: 3x  |  Lineage: dim_film, dim_customer, fact     │  │
│   └───────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘

Data Flow:
Film.csv ──┐
           ├──▶ /Volumes/.../raw/ ──▶ /staging/ ──▶ /cleaned/ ──▶ Delta Tables
Customer.csv ──┘                                                       │
Ticket.csv ────┘                                                       ▼
                                                              Analytics Output
                                                           (delta/analytics_*)
```

### Các service và công nghệ

| Service | Notebook | Công nghệ | Đầu ra |
|---------|----------|-----------|--------|
| Ingestion | `01_ingestion.ipynb` | PySpark, DBFS Volume | Parquet staging |
| ETL | `02_etl.ipynb` | PySpark SQL, Delta | Cleaned Parquet |
| Storage | `03_delta_lake.ipynb` | Delta Lake, Unity Catalog | Managed Tables |
| Analytics | `04_analytics.ipynb` | Spark SQL, Matplotlib | KPI + Charts |

---

## Cấu trúc dự án

```
/
├── README.md
├── bao_cao_giua_ky.md          # Báo cáo tiến độ giữa kỳ
│
├── databricks/
│   └── notebooks/
│       ├── 01_ingestion.ipynb  # Service 1: Thu thập & kiểm tra dữ liệu
│       ├── 02_etl.ipynb        # Service 2: Làm sạch & chuyển đổi
│       ├── 03_delta_lake.ipynb # Service 3: Lưu trữ Delta Lake
│       └── 04_analytics.ipynb  # Service 4: Phân tích & trực quan hóa
│
└── data/                       # (không commit lên GitHub — dùng Databricks Volume)
    ├── Film.csv
    ├── Customer.csv
    └── Ticket.csv
```

> **Lưu ý:** File CSV không được commit lên GitHub. Upload trực tiếp lên Databricks Volume theo hướng dẫn bên dưới.

---

## Dữ liệu đầu vào

### Film.csv

Phân cách: `;` | Encoding: UTF-8 | 17 bản ghi

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `show_id` | string | Mã phim |
| `film name` | string | Tên phim |
| `director` | string | Đạo diễn |
| `cast` | string | Diễn viên |
| `country` | string | Quốc gia sản xuất |
| `release_year` | int | Năm phát hành |
| `rating` | string | Phân loại (PG, R, ...) |
| `duration` | string | Thời lượng (phút) |
| `listed_in` | string | Thể loại |
| `Description` | string | Mô tả nội dung |

### Customer.csv

Phân cách: `;` | Encoding: UTF-8 | 4,479 bản ghi

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `customerid` | string | Mã khách hàng |
| `DOB` | string | Ngày sinh (dd/MM/yyyy) |
| `gender` | string | Giới tính |
| `address` | string | Địa chỉ (đường/quận/thành phố) |
| `job` | string | Nghề nghiệp |

### Ticket.csv

Phân cách: `;` | Encoding: UTF-8 | 35,472 bản ghi

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `orderid` | string | Mã đơn hàng |
| `cashier` | string | Thu ngân |
| `saledate` | string | Thời điểm bán vé |
| `total` | double | Tổng tiền đơn hàng |
| `customerid` | string | Mã khách hàng |
| `ticketcode` | string | Mã vé |
| `date` | string | Ngày chiếu |
| `time` | string | Giờ chiếu |
| `slot` | string | Số ghế |
| `room` | int | Phòng chiếu |
| `film` | string | Tên phim |
| `slot type` | string | Loại ghế (ĐƠN/ĐÔI) |
| `ticket type` | string | Loại vé |
| `ticket price` | double | Giá vé |
| `popcorn` | string | Có mua bắp rang (Có/Không) |

---

## Hướng dẫn triển khai

### Yêu cầu

- Tài khoản **Databricks** (Community Edition hoặc có Unity Catalog)
- Quyền tạo Volume trong Unity Catalog

### Bước 1 — Upload dữ liệu lên Databricks

1. Truy cập Databricks workspace
2. Vào **Data** → **Volumes** → `workspace` → `default`
3. Tạo Volume tên `data_film_company` (nếu chưa có)
4. Upload 3 file: `Film.csv`, `Customer.csv`, `Ticket.csv`

Sau khi upload, dữ liệu nằm tại:
```
/Volumes/workspace/default/data_film_company/Film.csv
/Volumes/workspace/default/data_film_company/Customer.csv
/Volumes/workspace/default/data_film_company/Ticket.csv
```

### Bước 2 — Import Notebooks

1. Vào **Workspace** → chọn thư mục cá nhân
2. **Import** → chọn từng file `.ipynb` trong thư mục `databricks/notebooks/`
3. Thứ tự import: `01` → `02` → `03` → `04`

### Bước 3 — Chạy theo thứ tự

Chạy lần lượt từng notebook (mỗi notebook phụ thuộc vào output của notebook trước):

```
01_ingestion.ipynb  →  02_etl.ipynb  →  03_delta_lake.ipynb  →  04_analytics.ipynb
```

> **Lưu ý:** Chọn **Serverless** compute hoặc cluster có Spark 3.x + Delta Lake

### Bước 4 — (Tuỳ chọn) Tạo Databricks Job

1. Vào **Workflows** → **Jobs** → **Create Job**
2. Đặt tên: `Job for film data`
3. Thêm Task: chọn notebook `04_analytics.ipynb`
4. Compute: **Serverless**
5. Retries: 3 lần
6. Lưu và nhấn **Run now**

---

## Chi tiết từng service

### Service 1 — Ingestion (`01_ingestion.ipynb`)

**Nhiệm vụ:** Đọc CSV thô, kiểm tra chất lượng, ghi staging

**Xử lý chính:**
- Đọc 3 file CSV với encoding UTF-8, phân cách `;`
- Tự động chuẩn hóa tên cột (bỏ BOM, strip khoảng trắng)
- Hàm `ingestion_report()`: đếm null/empty theo từng cột
- Phát hiện vấn đề: Customer.DOB có 3 null, Ticket.saledate có 94 null

**Output:**
```
/Volumes/.../staging/film/      (Parquet)
/Volumes/.../staging/customer/  (Parquet)
/Volumes/.../staging/ticket/    (Parquet)
```

**Kết quả:**
```
✅ Film     :  17 rows
✅ Customer : 4,479 rows
✅ Ticket   : 35,472 rows
```

---

### Service 2 — ETL (`02_etl.ipynb`)

**Nhiệm vụ:** Làm sạch, làm giàu, chuyển đổi sang Star Schema

**DIM_CUSTOMER** — Xử lý:
```python
# Parse DOB → age → age_group
.withColumn("dob", F.to_date("dob", "dd/MM/yyyy"))
.withColumn("age_2019", (2019 - F.year("dob")).cast(IntegerType()))
.withColumn("age_group",
    F.when(age < 20, "Dưới 20")
     .when(age < 30, "20-29") ...)

# Tách địa chỉ
.withColumn("district", F.split("address", "/").getItem(1))
.withColumn("city",     F.split("address", "/").getItem(2))

# Phân loại Online/Offline
.withColumn("customer_type",
    F.when(F.col("customer_id").startswith("WEBS"), "Online")
     .otherwise("Offline"))
```

**DIM_DATE** — Tạo từ ticket data:
- 30 ngày duy nhất (01/05/2019 → 31/05/2019)
- Cột: `day`, `month`, `year`, `quarter`, `day_of_week`, `day_name`, `is_weekend`

**FACT_TICKET** — Xử lý:
```python
# Phân khung giờ chiếu
.withColumn("show_session",
    F.when(show_hour < 12, "Sáng")
     .when(show_hour < 17, "Chiều")
     .when(show_hour < 20, "Tối sớm")
     .otherwise("Tối muộn"))

# Cờ logic
.withColumn("has_popcorn",    F.when(popcorn == "Có", True).otherwise(False))
.withColumn("is_couple_seat", F.when(slot_type == "ĐÔI", True).otherwise(False))
```

**Output:**
```
/Volumes/.../cleaned/dim_film/      (Parquet)
/Volumes/.../cleaned/dim_customer/  (Parquet)
/Volumes/.../cleaned/dim_date/      (Parquet)
/Volumes/.../cleaned/fact_ticket/   (Parquet)
```

---

### Service 3 — Storage / Delta Lake (`03_delta_lake.ipynb`)

**Nhiệm vụ:** Ghi vào Unity Catalog Managed Tables (Delta format)

**Ghi Delta với partitioning:**
```python
fact_ticket.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("sale_year", "sale_month") \
    .saveAsTable("workspace.default.fact_ticket")
```

**Tính năng Delta đã kiểm tra:**

| Tính năng | Lệnh | Kết quả |
|-----------|------|--------|
| ACID Write | `saveAsTable` | ✅ |
| OPTIMIZE | `OPTIMIZE workspace.default.fact_ticket` | ✅ Compaction hoàn thành |
| Time Travel | `VERSION AS OF 0` | ✅ 35,471 rows |
| Partition | `sale_year`, `sale_month` | ✅ |

**Bảng đã đăng ký trong Metastore:**
```sql
SHOW TABLES IN workspace.default;
-- dim_customer, dim_date, dim_film, fact_ticket
```

---

### Service 4 — Analytics (`04_analytics.ipynb`)

**Nhiệm vụ:** Tính KPI, vẽ biểu đồ, xuất kết quả về Delta

**6 nhóm KPI:**

| # | Nhóm | Chỉ số chính |
|---|------|-------------|
| 1 | Tổng quan | Tổng đơn, vé, doanh thu, giá TB |
| 2 | Doanh thu | Theo ngày, ngày thường vs cuối tuần |
| 3 | Phim | Top 10 phim, phân bố thể loại/rating |
| 4 | Khách hàng | Giới tính, nhóm tuổi, quận/huyện |
| 5 | Vé & Suất chiếu | Loại ghế, khung giờ, phòng chiếu |
| 6 | Bắp rang | Tương quan doanh thu, tỷ lệ mua |

**Output Delta:**
```
/Volumes/.../delta/analytics_revenue_daily/
/Volumes/.../delta/analytics_top_films/
/Volumes/.../delta/analytics_customer_age/
```

---

## Mô hình dữ liệu

### Star Schema

```
                    ┌─────────────────┐
                    │    DIM_FILM     │
                    ├─────────────────┤
                    │ film_name  (PK) │
                    │ country         │
                    │ director        │
                    │ rating          │
                    │ duration (int)  │
                    └────────┬────────┘
                             │ JOIN film_name
┌──────────────┐             │             ┌─────────────────────────────────┐
│   DIM_DATE   │             │             │          FACT_TICKET             │
├──────────────┤             │             ├─────────────────────────────────┤
│ sale_date PK │─────────────┼─────────────│ order_id       (PK)             │
│ day          │  JOIN date  │  JOIN cust  │ ticket_id                       │
│ month        │             │             │ customer_id    (FK → DIM_CUST)  │
│ year         │             │             │ film_name      (FK → DIM_FILM)  │
│ quarter      │             │             │ sale_date      (FK → DIM_DATE)  │
│ day_name     │             │             │ ticket_price                    │
│ is_weekend   │             │             │ total                           │
└──────────────┘             │             │ show_session                    │
                             │             │ slot_type / is_couple_seat      │
                    ┌────────┴────────┐    │ has_popcorn                     │
                    │  DIM_CUSTOMER   │    │ room / slot                     │
                    ├─────────────────┤    │ is_weekend                      │
                    │ customer_id (PK)│    │ sale_year / sale_month (PART.)  │
                    │ dob / age_2019  │    └─────────────────────────────────┘
                    │ age_group       │
                    │ gender          │
                    │ district / city │
                    │ job             │
                    │ customer_type   │
                    └─────────────────┘
```

### Thống kê bảng

| Bảng | Số dòng | Số cột | Partition |
|------|---------|--------|-----------|
| `dim_film` | 17 | 5 | — |
| `dim_customer` | 4,479 | 9 | — |
| `dim_date` | 30 | 10 | — |
| `fact_ticket` | 35,471 | 21 | `sale_year`, `sale_month` |

---

## Kết quả phân tích

### Tổng quan — Tháng 5/2019

| Chỉ số | Giá trị |
|--------|--------|
| Tổng đơn hàng | 14,383 |
| Tổng vé bán | 35,471 |
| Tổng khách hàng | 4,478 |
| Số phim đang chiếu | 19 |
| Tổng doanh thu | **5,758,115,000 VNĐ** |
| Giá vé trung bình | 47,521 VNĐ |

### Phân bố khung giờ

| Khung giờ | Số vé | Tỷ lệ |
|-----------|-------|-------|
| Tối sớm (17–20h) | 12,384 | 34.9% |
| Chiều (12–17h) | 10,832 | 30.5% |
| Tối muộn (≥20h) | 8,174 | 23.0% |
| Sáng (<12h) | 4,081 | 11.5% |

### Loại ghế

| Loại | Số vé | Tỷ lệ |
|------|-------|-------|
| Ghế đơn (ĐƠN) | 34,060 | 96.0% |
| Ghế đôi (ĐÔI) | 1,411 | 4.0% |

### Nhóm tuổi khách hàng

| Nhóm | Số khách |
|------|---------|
| 20–29 | 2,981 |
| Dưới 20 | 1,120 |
| 30–39 | 311 |
| 40–49 | 55 |
| 50+ | 12 |

---

## Thành viên nhóm

| STT | Họ và tên          | MSSV     |
|-----|-----------         |------    |
| 1   | Trương Đăng Thông  | 22667231 | 
| 2   | Hồ Thiên Bảo       | 22001975  | 
| 3   | Lê Quang Đỉnh      | 22632311 | 

---

## Công nghệ sử dụng

| Công nghệ | Phiên bản | Mục đích |
|-----------|-----------|---------|
| Databricks | Unity Catalog | Nền tảng chính |
| Apache Spark (PySpark) | 3.x | Xử lý dữ liệu phân tán |
| Delta Lake | — | ACID storage, Time Travel |
| Spark SQL | — | Truy vấn và phân tích |
| Matplotlib / Pandas | — | Trực quan hóa |
| Python | 3.x | Ngôn ngữ lập trình |

---

*Học phần: Kiến trúc hướng dịch vụ và Điện toán đám mây — Báo cáo giữa kỳ*
