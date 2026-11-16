# Bảng Quy ước Đồng nhất Hệ thống (CONVENTIONS.md)

[Trang chủ repo](../README.md) · [Tech Stack](TECH_STACK.md) · [Vận hành](README.md) · [Kiến trúc](ARCHITECTURE.md) · [Hợp đồng v1](DATA_CONTRACTS.md) · [Thực nghiệm](EXPERIMENT_PLAN.md) · [Kế hoạch & Phân công](../PLAN.md) · [Checklist](PROJECT_CHECKLIST.md)

*Tài liệu chuẩn hóa duy nhất (Single Source of Truth - SSOT) dùng chung giữa Nguyễn Đức Thành và Phan Văn Pháp*

---

## 1. Nguyên tắc Chung & Môi trường Kỹ thuật

| Mục | Quy định bắt buộc | Giải thích / Ghi chú |
| :--- | :--- | :--- |
| **Python Version** | `>=3.11, <3.12` (chuẩn Python 3.11) | Chạy qua Python Launcher hoặc virtualenv `.venv` |
| **Java Version** | **JDK 17** (Temurin-17) | Biến môi trường `JAVA_HOME` phải trỏ đúng JDK 17 |
| **PySpark** | **3.5.7** | Cố định theo `pyproject.toml`, chạy local mode (`local[2]` khi test) |
| **PostgreSQL** | **PostgreSQL 17** (Local, port `5432`) | Database: `recommender`, User: `recommender` |
| **Encoding & Line Ending** | **UTF-8**, **LF** (Unix format) | Tuyệt đối không dùng CRLF trong mã nguồn và file dữ liệu |
| **Indentation** | **4 spaces** | Không dùng tab |
| **Linting & Formatting** | **Ruff** (`target-version = "py311"`) | Lệnh: `python -m ruff check .` & `python -m ruff format --check .` |
| **Type Hints** | Bắt buộc cho toàn bộ hàm/method công khai | `def func(param: str) -> dict[str, Any]:` |

---

## 2. Từ điển Tên biến & Trường dữ liệu (Field & Variable Names)

Mọi biến trong Python, trường JSON, cột Parquet và cột PostgreSQL **bắt buộc dùng chính xác các tên sau**, không tự ý viết tắt hoặc biến tấu:

| Tên trường / biến | Kiểu dữ liệu | Ý nghĩa nghiệp vụ | Ví dụ giá trị chuẩn |
| :--- | :--- | :--- | :--- |
| `product_id` | `str` (chuỗi) | Định danh sản phẩm dạng `<source>:<raw_id>` | `"fixture:p001"`, `"synthetic:p042"` |
| `name` | `str` (chuỗi) | Tên sản phẩm (không rỗng, $\le 512$ ký tự) | `"Bàn phím cơ không dây"` |
| `category_id` | `str` (chuỗi) | Mã danh mục sản phẩm (lowercase, $\le 128$ ký tự) | `"keyboard"`, `"mouse"`, `"laptop"` |
| `description` | `str \| None` | Mô tả chi tiết sản phẩm (nullable) | `"Bàn phím 75%, switch cơ red"` |
| `brand` | `str \| None` | Thương hiệu sản phẩm (nullable) | `"Keychron"`, `"Logitech"` |
| `image_url` | `str \| None` | URL hình ảnh sản phẩm (nullable) | `"https://example.com/img/p001.jpg"` |
| `product_url` | `str \| None` | Link trang chi tiết gốc (nullable) | `"https://example.com/products/p001"` |
| `attributes` | `dict[str, str]` | Thuộc tính kỹ thuật (object phẳng chuỗi $\rightarrow$ chuỗi) | `{"connector": "usb-c", "layout": "75"}` |
| `event_id` | `str` (UUID) | ID sự kiện (chuỗi UUID v4 lowercase có gạch ngang) | `"ed8f2fa8-9b81-43b6-8bd4-9b8115640866"` |
| `user_id` | `str` (chuỗi) | ID người dùng giả danh | `"u_demo_001"`, `"u_syn_0042"` |
| `session_id` | `str` (chuỗi) | ID phiên tương tác (1 phiên thuộc đúng 1 user) | `"s_demo_001"` |
| `cart_id` | `str \| None` | ID giỏ hàng (bắt buộc khi `add_to_cart`, duy nhất toàn cục) | `"c_demo_001"` |
| `event_type` | `str` (enum) | Loại hành vi: chỉ nhận `"view"`, `"click"`, `"add_to_cart"` | `"click"` |
| `event_at` | `str` (ISO 8601) | Thời điểm tương tác chuẩn UTC có đuôi `Z` | `"2026-11-10T04:00:00Z"` |
| `source` | `str` (enum) | Nguồn dữ liệu: catalog dùng prefix, event dùng `"synthetic"` / `"demo"` | `"synthetic"`, `"demo"` |
| `collected_at` | `str` (ISO 8601) | Thời điểm thu thập catalog chuẩn UTC | `"2026-10-01T00:00:00Z"` |
| `interaction_weight`| `int` / `float` | Trọng số tương tác tích lũy của cặp (user, product) | `7` (ví dụ 1 view + 1 click + 1 add) |
| `last_event_at` | `str` (ISO 8601) | Thời điểm tương tác gần nhất của cặp (user, product) | `"2026-11-10T05:30:00Z"` |
| `run_id` | `str` (chuỗi) | Định danh lần chạy batch training | `"run_20261110_01"` |
| `dataset_version` | `str` (chuỗi) | Định danh phiên bản dữ liệu Silver | `"v20261110"` |
| `kind` | `str` (enum) | Nhóm gợi ý: `"personalized"`, `"substitute"`, `"popular"` | `"personalized"` |
| `context_id` | `str` (chuỗi) | Ngữ cảnh: `user_id` (với personal), `product_id` (với sub), `"global"` (với popular) | `"u_demo_001"`, `"global"` |
| `rank` | `int` | Thứ hạng gợi ý (bắt đầu từ **1**) | `1`, `2`, `3` |
| `score` | `float` | Điểm dự đoán (số thực hữu hạn, cấm NaN/Infinity) | `0.8542` |
| `method` | `str` (enum) | Thuật toán: `"als"`, `"popularity"`, `"tfidf_substitute"`, `"tfidf_user"`, `"als_mmr"`, `"session_rerank"` | `"als"` |
| `antecedent_ids` | `list[str]` | Danh sách sản phẩm vế trái của luật giỏ hàng (sắp tăng dần) | `["fixture:p001", "fixture:p002"]` |
| `consequent_id` | `str` (chuỗi) | Sản phẩm vế phải được gợi ý mua kèm | `"fixture:p003"` |
| `support` | `float` | Độ hỗ trợ của luật FP-Growth | `0.05` |
| `confidence` | `float` | Độ tin cậy của luật FP-Growth | `0.65` |
| `lift` | `float` | Độ nâng của luật FP-Growth | `2.4` |

---

## 3. Trọng số Hành vi Chuẩn (Behavior Weights) - BẮT BUỘC CỐ ĐỊNH

Tuyệt đối **không được thay đổi** hoặc dùng số khác ở bất kỳ file nào:

$$\text{Weight}(\text{view}) = 1$$
$$\text{Weight}(\text{click}) = 2$$
$$\text{Weight}(\text{add\_to\_cart}) = 4$$

- Khi tổng hợp tương tác Gold: $\text{interaction\_weight} = \sum (\text{count} \times \text{weight})$
- Khi rerank theo phiên: các sự kiện trong phiên cũng áp dụng đúng tỷ lệ `1 : 2 : 4`.

---

## 4. Cấu hình Môi trường & File `.env`

Cả hai người dùng chung các key cấu hình sau trong `.env`:

```ini
# Thư mục gốc chứa Data Lake (tương đối với thư mục gốc repo khi chạy local)
DATA_LAKE_DIR=data

# Cấu hình MinIO S3 Object Storage (Data Lake chính thức)
MINIO_ENDPOINT=127.0.0.1:9000
MINIO_CONSOLE_URL=http://127.0.0.1:9001
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SECURE=false
DATA_LAKE_STORAGE=minio

# Kết nối PostgreSQL (User: recommender, Pass: do bạn đặt, DB: recommender)
DATABASE_URL=postgresql://recommender:change-me@127.0.0.1:5432/recommender

# Địa chỉ FastAPI endpoint
API_BASE_URL=http://127.0.0.1:8000

# Seed ngẫu nhiên cố định để tái lập kết quả thực nghiệm
RANDOM_SEED=42
```

- **Thứ tự ưu tiên nạp cấu hình**: Environment Variables hệ thống > File `.env` > Giá trị mặc định trong `src/recommender/config.py`.

---

## 5. Cấu trúc MinIO S3 Buckets & Đường dẫn File Dữ liệu

Data Lake được tổ chức theo 3 tầng chuẩn Medallion trên **MinIO Object Storage** (chuẩn giao thức S3A `s3a://<bucket>/...`) và đồng bộ ánh xạ với thư mục local `data/`:

| Tầng Data Lake | MinIO S3 Bucket & Path | Ánh xạ thư mục Local | Định dạng & Chuẩn nén |
| :--- | :--- | :--- | :--- |
| **Bronze** | `s3a://bronze/products/`<br>`s3a://bronze/events/` | `data/bronze/products/`<br>`data/bronze/events/` | JSON Lines (`.jsonl`), UTF-8, Append-only |
| **Silver** | `s3a://silver/products/<dataset_version>/`<br>`s3a://silver/events/<dataset_version>/` | `data/silver/products/`<br>`data/silver/events/` | Apache Parquet (`.parquet`), nén Snappy |
| **Gold** | `s3a://gold/<run_id>/` | `data/gold/<run_id>/` | Apache Parquet (`.parquet`), nén Snappy |

```text
minio/ (Bucket s3a:// hoặc local data/)
├── bronze/                         # File JSONL gốc UTF-8 (1 dòng = 1 object JSON)
│   ├── products/                   # Catalog thu thập (products_<source>_<timestamp>.jsonl)
│   └── events/                     # Sự kiện (events_synthetic_... hoặc events_demo_...)
├── silver/                         # Parquet sạch, nén Snappy, schema chuẩn
│   ├── products/                   # Silver catalog Parquet
│   └── events/                     # Silver events Parquet
└── gold/                           # Kết quả batch theo từng run_id
    └── <run_id>/
        ├── user_interactions.parquet      # Cặp (user_id, product_id, interaction_weight)
        ├── recommendation_items.parquet   # Top-K dự đoán (personalized, substitute, popular)
        ├── seen_products.parquet          # Lịch sử sản phẩm user đã tương tác để lọc trùng
        ├── association_rules.parquet      # Luật FP-Growth cho giỏ hàng
        └── manifest.json                  # Metadata, số dòng, metrics, seed
```

- **Test Fixtures** (dùng cho unit test offline):
  - `tests/fixtures/products.jsonl`: ~20 sản phẩm mẫu (3-4 categories).
  - `tests/fixtures/events.jsonl`: ~50 events mẫu (view/click/add_to_cart có cart_id).

---

## 6. Định dạng Thời gian & Quy tắc ID

1. **Chuẩn thời gian (Wire format)**:
   - Toàn bộ timestamp trao đổi qua JSON / Parquet / API phải là **ISO 8601 UTC** với hậu tố `Z`, ví dụ: `2026-11-10T04:00:00Z`.
   - Trong PostgreSQL lưu kiểu `TIMESTAMPTZ` (Timezone luôn đặt UTC).
   - Trong PySpark lưu kiểu `TimestampType()`.
2. **Quy tắc ID**:
   - `product_id`: Bắt buộc có prefix nguồn: `<source>:<raw_id>`. Không chứa khoảng trắng.
   - `event_id`: Bắt buộc là UUID v4 hợp lệ dạng chữ thường (lowercase).
   - `cart_id`: Khi `event_type == "add_to_cart"`, trường `cart_id` **bắt buộc không được null/rỗng**. Với `view` và `click`, `cart_id` có thể là `null`.
   - `session_id`: Mỗi phiên thuộc về 1 `user_id`. Không được dùng 1 `session_id` cho 2 `user_id` khác nhau.

---

## 7. Schema Cơ sở Dữ liệu PostgreSQL (7 Bảng Chuẩn)

### 7.1. Bảng `products` (Catalog phục vụ)
```sql
CREATE TABLE products (
    product_id   TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    category_id  TEXT NOT NULL,
    source       TEXT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    description  TEXT,
    brand        TEXT,
    image_url    TEXT,
    product_url  TEXT,
    attributes   JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX idx_products_category ON products(category_id);
```

### 7.2. Bảng `events` (Nhật ký hành vi người dùng)
```sql
CREATE TABLE events (
    event_id    UUID PRIMARY KEY,
    user_id     TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    product_id  TEXT NOT NULL REFERENCES products(product_id),
    event_type  TEXT NOT NULL CHECK (event_type IN ('view', 'click', 'add_to_cart')),
    event_at    TIMESTAMPTZ NOT NULL,
    cart_id     TEXT,
    source      TEXT NOT NULL CHECK (source IN ('synthetic', 'demo')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT chk_cart_id_on_add CHECK (event_type != 'add_to_cart' OR cart_id IS NOT NULL)
);
CREATE INDEX idx_events_session ON events(session_id, event_at DESC);
CREATE INDEX idx_events_user ON events(user_id, event_at DESC);
```

### 7.3. Bảng `batch_runs` (Quản lý các đợt chạy Gold)
```sql
CREATE TABLE batch_runs (
    run_id      TEXT PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    status      TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
    metrics     JSONB NOT NULL DEFAULT '{}'::jsonb
);
```

### 7.4. Bảng `serving_state` (Con trỏ snapshot đang phục vụ - Singleton)
```sql
CREATE TABLE serving_state (
    id             INT PRIMARY KEY CHECK (id = 1),
    active_run_id  TEXT NOT NULL REFERENCES batch_runs(run_id),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
```

### 7.5. Bảng `recommendation_items` (Top-K kết quả gợi ý)
```sql
CREATE TABLE recommendation_items (
    run_id      TEXT NOT NULL REFERENCES batch_runs(run_id) ON DELETE CASCADE,
    kind        TEXT NOT NULL CHECK (kind IN ('personalized', 'substitute', 'popular')),
    context_id  TEXT NOT NULL,
    product_id  TEXT NOT NULL REFERENCES products(product_id),
    rank        INT NOT NULL CHECK (rank >= 1),
    score       DOUBLE PRECISION NOT NULL,
    method      TEXT NOT NULL,
    PRIMARY KEY (run_id, kind, context_id, product_id)
);
CREATE INDEX idx_rec_lookup ON recommendation_items(run_id, kind, context_id, rank);
```

### 7.6. Bảng `seen_products` (Lịch sử đã tương tác trong batch để lọc trùng)
```sql
CREATE TABLE seen_products (
    user_id             TEXT NOT NULL,
    product_id          TEXT NOT NULL REFERENCES products(product_id),
    last_interacted_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, product_id)
);
CREATE INDEX idx_seen_user ON seen_products(user_id);
```

### 7.7. Bảng `association_rules` (Luật mua kèm FP-Growth)
```sql
CREATE TABLE association_rules (
    run_id          TEXT NOT NULL REFERENCES batch_runs(run_id) ON DELETE CASCADE,
    antecedent_ids  TEXT[] NOT NULL,
    consequent_id   TEXT NOT NULL REFERENCES products(product_id),
    support         DOUBLE PRECISION NOT NULL,
    confidence      DOUBLE PRECISION NOT NULL,
    lift            DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (run_id, antecedent_ids, consequent_id)
);
CREATE INDEX idx_rules_lookup ON association_rules(run_id, confidence DESC, lift DESC);
```

---

## 8. Hợp đồng API v1 Chuẩn (FastAPI Routes)

Tất cả các route nghiệp vụ đều nằm dưới prefix `/api/v1` (riêng `/health` ở root):

| Endpoint | Method | Query / Body Params | Response Status & Body | Ghi chú nghiệp vụ |
| :--- | :--- | :--- | :--- | :--- |
| `/health` | `GET` | Không | `200` `{"status":"ok"}` | Kiểm tra server sống |
| `/api/v1/products` | `GET` | `limit: int = 20`<br>`offset: int = 0` | `200` `{"items": [...]}` | Danh sách catalog, sắp theo `product_id` tăng dần |
| `/api/v1/products/{product_id}` | `GET` | Path param `product_id` | `200` (Product object)<br>`404` `{"detail":"Not found"}` | Chi tiết 1 sản phẩm |
| `/api/v1/events` | `POST` | Event JSON Object (`source="demo"`) | `201` `{"event_id":"..."}` (Mới)<br>`200` (Trùng ID & payload)<br>`409` (Trùng ID khác payload)<br>`404` (Sản phẩm không có)<br>`422` (Sai schema / thiếu cart_id) | Nhận log từ Streamlit UI, đảm bảo tính idempotent |
| `/api/v1/recommendations/personalized` | `GET` | `user_id: str`<br>`session_id: str \| None = None`<br>`limit: int = 10` | `200` `{"run_id":"...","items":[...]}`<br>`503` (Chưa có snapshot) | Loại bỏ `seen_products`, fallback `popular` nếu cold-start; nếu có `session_id` thì rerank theo phiên |
| `/api/v1/recommendations/substitutes` | `GET` | `product_id: str`<br>`limit: int = 10` | `200` `{"run_id":"...","items":[...]}`<br>`404` (Không thấy item)<br>`503` (Chưa có snapshot) | Cùng `category_id`, loại trừ chính sản phẩm đang xem |
| `/api/v1/recommendations/complements` | `POST` | Body: `{"product_ids": [...], "limit": 10}` | `200` `{"run_id":"...","items":[...]}`<br>`404` (Item không tồn tại)<br>`503` (Chưa có snapshot) | Lọc luật có `antecedent_ids` $\subseteq$ giỏ, loại bỏ item đã có trong giỏ; giỏ rỗng trả `[]` |

---

## 9. Quy tắc Xếp hạng & Tie-Breaking

Khi điểm số gợi ý hoặc độ tương đồng cosine bằng nhau:
1. **Tie-Breaking cố định**: Luôn sắp xếp theo `product_id` tăng dần theo thứ tự chuỗi Unicode (ASCII / Collation `C`).
2. **Quy tắc ưu tiên luật giỏ hàng**:
   - Bước 1: Khớp `antecedent_ids` nằm hoàn toàn trong giỏ hàng hiện tại.
   - Bước 2: Sắp xếp theo `confidence` giảm dần.
   - Bước 3: Nếu `confidence` bằng nhau $\rightarrow$ sắp xếp theo `lift` giảm dần.
   - Bước 4: Nếu `lift` bằng nhau $\rightarrow$ sắp xếp theo `consequent_id` (tức `product_id`) tăng dần.
   - Luôn loại bỏ các sản phẩm đã nằm trong giỏ hàng hiện tại trước khi lấy Top-K.

---

## 10. Mốc Thời gian Chia Thực nghiệm (Experiment Split Cutoffs)

Cả hai người dùng chung 2 mốc thời gian UTC cố định để chia tập:
- `split_val_time`: `"2026-10-25T00:00:00Z"`
- `split_test_time`: `"2026-11-01T00:00:00Z"`

- **Tập Train**: Toàn bộ events trước `split_val_time`.
- **Tập Validation**: Events từ `split_val_time` đến trước `split_test_time` (dùng chỉnh tham số $\lambda$ của MMR, rank của ALS, minSupport/minConfidence của FP-Growth).
- **Tập Test**: Events từ `split_test_time` trở đi (đánh giá cuối cùng, đo Recall@10, NDCG@10).
- **Nguyên tắc vàng**: Giỏ hàng (`cart_id`) đi qua mốc thời gian không được xé đôi; giữ nguyên giỏ hàng cho tập xảy ra sau. Tuyệt đối không để dữ liệu tương lai rò rỉ vào tập Train.

---

## 11. Quy ước Đặt tên Code & Git Commit

### 11.1. Quy ước đặt tên trong mã nguồn Python
- **Tên file / module**: `snake_case.py` (ví dụ: `synthetic.py`, `importer.py`, `popularity.py`)
- **Tên biến / hàm**: `snake_case` (ví dụ: `product_id`, `calculate_ndcg()`, `get_active_snapshot()`)
- **Tên lớp (Class)**: `PascalCase` (ví dụ: `ProductSchema`, `EventRecord`, `ALSRecommender`)
- **Hằng số (Constant)**: `UPPER_SNAKE_CASE` (ví dụ: `DEFAULT_RANDOM_SEED`, `MAX_RETRIES`)

### 11.2. Quy ước Git Branch & Commit
- **Nhánh (Branch)**:
  - Tính năng mới: `feat/<tên-ngắn>` (ví dụ: `feat/synthetic-generator`, `feat/fastapi-serving`)
  - Sửa lỗi: `fix/<tên-ngắn>` (ví dụ: `fix/utc-timestamp-parser`)
  - Tài liệu: `docs/<tên-ngắn>` (ví dụ: `docs/update-conventions`)
- **Quy tắc Commit Message**: `type(scope): message`
  - `feat(ingestion): add synthetic behavior generator with faker`
  - `feat(pipeline): implement pyspark bronze to silver etl`
  - `fix(api): handle idempotency on duplicate event submission`
  - `test(contracts): add validation tests for cart_id on add_to_cart`
  - `docs(conventions): synchronize unified standards between team members`

---

*Mọi thắc mắc hoặc đề xuất thay đổi bất kỳ quy ước nào trong file này phải được thảo luận và thống nhất giữa Nguyễn Đức Thành và Phan Văn Pháp trước khi cập nhật mã nguồn.*

