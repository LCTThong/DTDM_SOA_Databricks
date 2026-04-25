# BÁO CÁO TIẾN ĐỘ GIỮA KỲ
## Học phần: Kiến trúc hướng dịch vụ và Điện toán đám mây

---

## SLIDE 1 — TRANG BÌA

**Tên đề tài:**
> Xây dựng hệ thống phân tích dữ liệu rạp phim theo kiến trúc hướng dịch vụ (SOA) trên nền tảng Databricks

**Học phần:** Kiến trúc hướng dịch vụ và Điện toán đám mây

**Nền tảng Cloud:** Databricks (Unity Catalog)

**Link GitHub:** `https://github.com/LCTThong/DTDM_SOA_Databricks.git`

---

## SLIDE 2 — THÔNG TIN NHÓM

### Danh sách thành viên

| STT | Họ và tên          | MSSV     |
|-----|-----------         |------    |
| 1   | Trương Đăng Thông  | 22667231 | 
| 2   | Hồ Thiên Bảo       | 22001975  | 
| 3   | Lê Quang Đỉnh      | 22632311 | 

### Mục tiêu hệ thống

Xây dựng pipeline xử lý dữ liệu bán vé rạp phim theo mô hình **SOA (Service-Oriented Architecture)** trên Databricks, bao gồm:

- **Ingestion Service**: Thu thập và kiểm tra chất lượng dữ liệu từ 3 nguồn CSV (Film, Customer, Ticket)
- **ETL Service**: Làm sạch, chuẩn hóa, chuyển đổi dữ liệu thành mô hình Star Schema (DIM + FACT)
- **Storage Service**: Lưu trữ bền vững với Delta Lake (ACID, Time Travel, OPTIMIZE)
- **Analytics Service**: Tính toán KPI, trực quan hóa xu hướng doanh thu, hành vi khách hàng

**Dữ liệu đầu vào:**

| File | Số dòng | Mô tả |
|------|---------|-------|
| `Film.csv` | 17 | Danh mục phim đang chiếu |
| `Customer.csv` | 4,479 | Thông tin khách hàng |
| `Ticket.csv` | 35,472 | Giao dịch mua vé tháng 5/2019 |

---

## SLIDE 3 — KIẾN TRÚC HỆ THỐNG TỔNG QUAN

### Mô hình SOA — Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATABRICKS PLATFORM                       │
│                     (Unity Catalog Workspace)                    │
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐    │
│  │   CSV Files  │     │   Staging    │     │  Delta Lake  │    │
│  │  Film.csv    │     │  (Parquet)   │     │  (UC Tables) │    │
│  │  Customer.csv│     │  /staging/   │     │  dim_film    │    │
│  │  Ticket.csv  │     │    film/     │     │  dim_customer│    │
│  └──────┬───────┘     │    cust/     │     │  dim_date    │    │
│         │             │    ticket/   │     │  fact_ticket │    │
│         ▼             └──────┬───────┘     └──────┬───────┘    │
│  ┌──────────────┐            │                    │            │
│  │  INGESTION   │────────────┘            ┌───────▼───────┐   │
│  │   SERVICE    │                         │  ANALYTICS    │   │
│  │ 01_ingestion │     ┌──────────────┐    │   SERVICE     │   │
│  │   .ipynb     │     │ ETL SERVICE  │    │ 04_analytics  │   │
│  └──────────────┘     │  02_etl      │    │    .ipynb     │   │
│                        │   .ipynb    │    │               │   │
│                        │             │    │  KPIs:        │   │
│                        │ DIM_CUSTOMER│    │ - Doanh thu   │   │
│                        │ DIM_FILM    │    │ - Top phim    │   │
│                        │ DIM_DATE    │    │ - Khách hàng  │   │
│                        │ FACT_TICKET │    │ - Khung giờ   │   │
│                        └──────┬──────┘    └───────────────┘   │
│                               │                                 │
│                        ┌──────▼──────┐                         │
│                        │  STORAGE    │                         │
│                        │  SERVICE    │                         │
│                        │ 03_delta    │                         │
│                        │  _lake.ipynb│                         │
│                        └─────────────┘                         │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              DATABRICKS JOB — "Job for film data"        │   │
│  │  Task: run_film_analytics → 04_analytics.ipynb           │   │
│  │  Compute: Serverless  |  Retry: 3x  |  Lineage: 3 tables │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Star Schema — Mô hình dữ liệu

```
         ┌────────────────┐
         │   DIM_FILM     │
         │ - film_name PK │
         │ - country      │
         │ - director     │
         │ - rating       │
         │ - duration     │
         └───────┬────────┘
                 │
┌────────────┐   │   ┌──────────────────────────────────────────┐
│  DIM_DATE  │   │   │              FACT_TICKET                 │
│ - sale_date│───┼───│ - order_id, ticket_id, customer_id       │
│ - day/month│   │   │ - film_name (FK → DIM_FILM)              │
│ - quarter  │   │   │ - sale_date (FK → DIM_DATE)              │
│ - is_weekend│  │   │ - customer_id (FK → DIM_CUSTOMER)        │
└────────────┘   │   │ - ticket_price, total, has_popcorn       │
                 │   │ - show_session, slot_type, room           │
                 │   └──────────────────────────────────────────┘
         ┌───────┴────────┐
         │ DIM_CUSTOMER   │
         │ - customer_id  │
         │ - age_group    │
         │ - gender       │
         │ - district/city│
         │ - customer_type│
         └────────────────┘
```

---

## SLIDE 4 — TIẾN ĐỘ TRIỂN KHAI TRÊN DATABRICKS

### Workspace & Cluster

| Thành phần | Trạng thái | Chi tiết |
|-----------|-----------|---------|
| Workspace | ✅ Hoàn thành | Unity Catalog — `workspace.default` |
| Volume | ✅ Hoàn thành | `/Volumes/workspace/default/data_film_company/` |
| Compute | ✅ Hoàn thành | Serverless (không cần quản lý cluster) |
| Job | ✅ Hoàn thành | "Job for film data" — Job ID: 824090450688263 |

### Notebooks đã tạo

| Notebook | Nhiệm vụ | Kết quả |
|----------|---------|--------|
| `01_ingestion.ipynb` | Đọc CSV, kiểm tra schema & null | 35,472 vé, 4,479 KH, 17 phim |
| `02_etl.ipynb` | Làm sạch, tạo DIM/FACT tables | Star Schema 4 bảng |
| `03_delta_lake.ipynb` | Ghi Delta, OPTIMIZE, Time Travel | 4 Delta tables trong Metastore |
| `04_analytics.ipynb` | Tính KPI, vẽ biểu đồ | 6 nhóm KPI, dashboard |

---

## SLIDE 5 — DATA INGESTION (Notebook 01)

### Kết quả đọc dữ liệu thô

**Ingestion Service** đọc 3 file CSV từ Unity Catalog Volume:

```
Path: /Volumes/workspace/default/data_film_company/
```

| Bảng | Số dòng | Số cột | Null/lỗi |
|------|---------|--------|----------|
| `Film.csv` | 17 | 10 | 0 |
| `Customer.csv` | 4,479 | 5 | DOB: 3 dòng ⚠️ |
| `Ticket.csv` | 35,472 | 15 | saledate/total/popcorn: 94 dòng ⚠️ |

**Output:**
- Dữ liệu staging lưu tại: `/Volumes/.../staging/` (Parquet format)
- Báo cáo null values tự động theo từng cột

> 📸 *[Chèn ảnh chụp màn hình: Databricks notebook output — ingestion_report]*

---

## SLIDE 6 — ETL SERVICE (Notebook 02)

### Quy trình làm sạch & chuyển đổi

**DIM_CUSTOMER** (4,479 rows):
- Parse DOB → tính `age_2019`, phân nhóm `age_group` (Dưới 20 / 20-29 / 30-39 / 40-49 / 50+)
- Tách địa chỉ → `street`, `district`, `city`
- Phân loại `customer_type`: Online (prefix "WEBS") / Offline

**DIM_FILM** (17 rows):
- Chuẩn hóa tên cột, extract số phút từ cột `duration`

**DIM_DATE** (30 rows):
- Generate từ ticket data: day, month, year, quarter, `is_weekend`, day_name

**FACT_TICKET** (35,471 rows):
- Parse `sale_date`, tính `show_session` (Sáng / Chiều / Tối sớm / Tối muộn)
- Cast kiểu số cho `ticket_price`, `total`, `room`
- Cờ `has_popcorn`, `is_couple_seat`, `is_weekend`

**Output:** Cleaned data → `/Volumes/.../cleaned/` (Parquet)

> 📸 *[Chèn ảnh chụp màn hình: ETL output — dim/fact table schema]*

---

## SLIDE 7 — DELTA LAKE & STORAGE SERVICE (Notebook 03)

### Lưu trữ với Delta Lake (ACID)

Tất cả 4 bảng được ghi vào **Unity Catalog Managed Tables** định dạng Delta:

```sql
-- Ví dụ ghi có overwriteSchema
dim_film.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("workspace.default.dim_film")
```

| Bảng Delta | Số dòng | Partition |
|-----------|---------|-----------|
| `dim_film` | 17 | — |
| `dim_customer` | 4,479 | — |
| `dim_date` | 30 | — |
| `fact_ticket` | 35,471 | `sale_year`, `sale_month` |

### Tính năng Delta đã kiểm tra

| Tính năng | Kết quả |
|-----------|--------|
| ACID Write | ✅ `overwriteSchema` thành công |
| OPTIMIZE | ✅ Compaction fact_ticket hoàn thành |
| Time Travel | ✅ `VERSION AS OF 0` → 35,471 rows |
| Partitioning | ✅ Partition theo `sale_year`, `sale_month` |

> 📸 *[Chèn ảnh chụp màn hình: SHOW TABLES workspace.default — 4 bảng Delta]*

---

## SLIDE 8 — ANALYTICS SERVICE & KPI (Notebook 04)

### Summary tổng quan — Tháng 5/2019

| KPI | Giá trị |
|-----|--------|
| Tổng đơn hàng | 14,383 |
| Tổng vé bán | 35,471 |
| Tổng khách hàng | 4,478 |
| Số phim chiếu | 19 |
| **Tổng doanh thu** | **5,758,115,000 VNĐ** |
| Giá vé trung bình | 47,521 VNĐ |
| Khoảng thời gian | 01/05/2019 → 31/05/2019 |

### Phân bố khung giờ chiếu

| Khung giờ | Số vé | Tỷ lệ |
|-----------|-------|-------|
| Tối sớm (17–20h) | 12,384 | 34.9% |
| Chiều (12–17h) | 10,832 | 30.5% |
| Tối muộn (≥20h) | 8,174 | 23.0% |
| Sáng (<12h) | 4,081 | 11.5% |

### Loại slot

| Loại | Số vé | Tỷ lệ |
|------|-------|-------|
| Ghế đơn (ĐƠN) | 34,060 | 96.0% |
| Ghế đôi (ĐÔI) | 1,411 | 4.0% |

> 📸 *[Chèn ảnh chụp màn hình: Dashboard matplotlib — doanh thu theo ngày, top phim, khung giờ]*

---

## SLIDE 9 — DATABRICKS JOB & MINH CHỨNG

### Job đã tạo: "Job for film data"

| Thuộc tính | Giá trị |
|-----------|--------|
| Job ID | 824090450688263 |
| Task | `run_film_analytics` → `04_analytics.ipynb` |
| Compute | Serverless |
| Retry | 3 lần (4 tổng attempts) |
| Lineage | 3 upstream tables |
| Performance Optimized | Bật |

> 📸 *[Chèn ảnh chụp màn hình: Databricks Jobs UI — Task view]*

> 📸 *[Chèn ảnh chụp màn hình: Databricks Jobs UI — Runs tab (job đã chạy thành công)]*

### Link GitHub

> 🔗 **GitHub Repository:** `https://github.com/LCTThong/DTDM_SOA_Databricks.git`

Cấu trúc repo:
```
/
├── databricks/
│   └── notebooks/
│       ├── 01_ingestion.ipynb
│       ├── 02_etl.ipynb
│       ├── 03_delta_lake.ipynb
│       └── 04_analytics.ipynb
├── Customer.csv
├── Film.csv
└── Ticket.csv
```

---

## SLIDE 10 — KHÓ KHĂN VÀ VẤN ĐỀ TỒN ĐỌNG

### Lỗi kỹ thuật đã gặp

| # | Vấn đề | Nguyên nhân | Giải pháp |
|---|--------|------------|-----------|
| 1 | `/FileStore/tables/` bị vô hiệu hóa | Unity Catalog không cho phép DBFS cũ | Chuyển sang Unity Catalog Volume `/Volumes/...` |
| 2 | BOM character trong tên cột CSV | File CSV có encoding UTF-8 với BOM | Dùng `.lstrip('﻿')` khi đọc header |
| 3 | `multiLine` cần bật cho Film.csv | Cột `description` có nội dung xuống dòng | Thêm `.option("multiLine", True)` |
| 4 | `DESCRIBE HISTORY` không hiển thị output | Serverless cluster cache kết quả khác | Dùng `display()` thay vì `.show()` |
| 5 | Gender toàn bộ = "Other" | Format giá trị gender trong CSV khác với kỳ vọng | Cần kiểm tra giá trị thực trong CSV và cập nhật mapping |

### Phần chưa hoàn thành

| Hạng mục | Trạng thái | Nguyên nhân |
|---------|-----------|------------|
| API endpoint phục vụ kết quả analytics | ⏳ Chưa bắt đầu | Ưu tiên hoàn thiện pipeline dữ liệu trước |
| Dashboard tương tác (Databricks SQL / Streamlit) | ⏳ Chưa bắt đầu | Đang trong kế hoạch cuối kỳ |
| Fix mapping gender trong ETL | ⚠️ Chưa sửa | Cần xác nhận format dữ liệu gốc |
| Job schedule / trigger tự động | ⏳ Chưa cấu hình | Hiện tại chạy thủ công |

---

## SLIDE 11 — KẾ HOẠCH ĐẾN CUỐI KỲ

### Danh sách công việc còn lại

| # | Công việc | Độ ưu tiên | Người phụ trách |
|---|-----------|-----------|----------------|
| 1 | Fix ETL — gender mapping | 🔴 Cao | [Thành viên 2] |
| 2 | Thiết lập Job schedule (trigger tự động) | 🟡 Trung bình | [Thành viên 1] |
| 3 | Xây dựng Databricks SQL Dashboard | 🔴 Cao | [Thành viên 3] |
| 4 | Phân tích nâng cao: cohort, retention, RFM | 🟡 Trung bình | [Thành viên 3] |
| 5 | Hoàn thiện GitHub (README, documentation) | 🟢 Thấp | [Thành viên 4] |
| 6 | Kiểm thử toàn pipeline end-to-end | 🔴 Cao | [Thành viên 5] |
| 7 | Chuẩn bị slide & báo cáo cuối kỳ | 🔴 Cao | Cả nhóm |

### Timeline dự kiến

```
Tháng 4/2025
├── Tuần 1–2 (01–14/04): Fix ETL bugs, hoàn thiện Delta pipeline
├── Tuần 3   (15–21/04): Xây dựng Databricks SQL Dashboard
└── Tuần 4   (22–30/04): Phân tích nâng cao, kiểm thử end-to-end

Tháng 5/2025
├── Tuần 1–2 (01–14/05): Tích hợp API / Serving layer (nếu có)
├── Tuần 3   (15–21/05): Hoàn thiện tài liệu, GitHub README
└── Tuần 4   (22–31/05): Chuẩn bị báo cáo cuối kỳ, demo
```

---

## SLIDE 12 — TỔNG KẾT TIẾN ĐỘ

### Mức độ hoàn thành giữa kỳ

| Hạng mục | Tiến độ |
|---------|--------|
| Ingestion Service | ████████████ 100% |
| ETL Service | ██████████░░ 85% |
| Storage / Delta Lake | ████████████ 100% |
| Analytics Service | ██████████░░ 80% |
| Job / Orchestration | ████████░░░░ 70% |
| Dashboard / Serving | ██░░░░░░░░░░ 15% |

**Tổng tiến độ ước tính: ~75%**

### Cam kết

- Hoàn thành 100% pipeline theo đúng kiến trúc SOA đã chốt
- Không thay đổi kiến trúc tổng thể (4 services: Ingestion → ETL → Storage → Analytics)
- Bổ sung Dashboard tương tác và tài liệu đầy đủ trước báo cáo cuối kỳ

---

**🔗 GitHub:** `https://github.com/LCTThong/DTDM_SOA_Databricks.git`

---

> *Báo cáo tiến độ giữa kỳ — Học phần Kiến trúc hướng dịch vụ và Điện toán đám mây*
> *Ngày: 21/04/2026*
