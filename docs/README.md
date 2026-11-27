# Hướng dẫn vận hành và tích hợp

[Trang chủ repo](../README.md) · [Tech Stack](TECH_STACK.md) · [Quy ước đồng nhất](CONVENTIONS.md) · [Kiến trúc](ARCHITECTURE.md) · [Hợp đồng v1](DATA_CONTRACTS.md) · [Thực nghiệm](EXPERIMENT_PLAN.md) · [Kế hoạch & Phân công](../PLAN.md) · [Checklist](PROJECT_CHECKLIST.md)

## 1. Trạng thái có thể chạy sau T07

Repo đã có cấu hình, schema Pydantic và fixture/test hợp đồng T02, CLI thu thập catalog Best Buy và sinh sự kiện T03, cùng nhánh Big Data/ML của Vai trò B: ETL PySpark Bronze → Silver (T04), Popularity/TF-IDF/Substitutes (T05), Implicit ALS/MMR/FP-Growth (T06), split + metric + benchmark (T07) và xuất Gold snapshot theo `run_id` cho importer T08. Chưa có importer, FastAPI hoặc Streamlit (T08–T11).

[README gốc](../README.md) là điểm bắt đầu cài môi trường và chỉ mục tài liệu. [PLAN.md](../PLAN.md) định nghĩa T01–T12; [PROJECT_CHECKLIST.md](PROJECT_CHECKLIST.md) theo dõi nguồn catalog, chia sẻ dataset, thực nghiệm và báo cáo P0–P15. T01 không tự đánh dấu hoàn tất kiểm tra triển khai hoặc review nhóm.

## 2. Chuẩn bị máy

Thực hiện các lệnh tại [hướng dẫn cài đặt gốc](../README.md): Python 3.11, JDK 17, PostgreSQL local, `.venv`, extras `pipeline,app,dev`, role/database và `.env`. Từ gốc repo dùng interpreter `.\.venv\Scripts\python.exe` để tránh chạy nhầm Python mặc định.

| Thành phần | Điều kiện kiểm tra |
| --- | --- |
| Java | `java -version` trả JDK 17; JAVA_HOME và PATH thống nhất. |
| Python | `.\.venv\Scripts\python.exe --version` trả 3.11.x. |
| Dependency | `.\.venv\Scripts\python.exe -m pip check` không có dependency lỗi. |
| Spark | Smoke check trong README gốc tạo session local, đếm ba dòng và dừng session. |
| PostgreSQL | Role recommender kết nối đúng database; không dùng tài khoản quản trị cho app. |
| Cấu hình | DATA_LAKE_DIR có quyền ghi, DATABASE_URL dùng mật khẩu local, API_BASE_URL đúng API, RANDOM_SEED=42. |

Module đọc `.env` thuộc T02, ưu tiên môi trường tiến trình. Chép file mẫu không tự cấu hình DB hay tạo bảng. Migration thuộc T08 và phải theo [DATA_CONTRACTS.md](DATA_CONTRACTS.md). Không commit `.env`, token, dữ liệu lớn hoặc log thật.

## 3. Thứ tự tích hợp các task

Đây là quy trình bàn giao cho task sau. Đường dẫn code bằng backtick là vị trí dự kiến theo kế hoạch. Khi task hoàn tất, PR bổ sung vào tài liệu này lệnh thực chạy đã kiểm chứng; không hướng dẫn gọi module chưa tồn tại như một tính năng sẵn có.

| Bước | Task / thành phần | Đầu vào → đầu ra và điều kiện chuyển bước |
| --- | --- | --- |
| 1 | T02: cấu hình, schema, fixture | Schema v1 → fixture nhỏ parse được, kiểm tra cart/UTC/UUID. |
| 2 | P2/T03: catalog, `ingestion/synthetic.py` | Catalog được phép hoặc fixture → Bronze JSONL, manifest/checksum, thống kê; cùng seed tái tạo dữ liệu. |
| 3 | T04: `pipeline/etl.py` | Bronze → Silver theo dataset_version, quality report; không phát hành conflict chưa xử lý. |
| 4 | P5: chia sẻ dataset | Gói Silver → máy B tải đúng version và kiểm tra checksum trước train. |
| 5 | T07, T04–T06: split và train | Silver → chia UTC/purge giỏ → interactions/baskets riêng cửa sổ → popularity và model. |
| 6 | T05–T07: Gold/đánh giá | Model → products/recommendation_items/seen_products/association_rules cùng run; validation chọn cấu hình, test giữ riêng. |
| 7 | T08: migration, `pipeline/importer.py` | DB trống → bảy bảng → kiểm tra và nạp Gold/kích hoạt nguyên tử; lỗi giữ snapshot cũ. |
| 8 | T09: `api/main.py` | Snapshot ready → health/catalog/event/ba nhóm gợi ý đúng hợp đồng. |
| 9 | T11: `ui/app.py` | API_BASE_URL → trang chủ/chi tiết/giỏ, ID ổn định, thao tác ghi event. |
| 10 | T10: session, `pipeline/export_events.py` | Event commit → GET có session cập nhật; xuất log → Bronze giữ UUID. |
| 11 | T12: runner và benchmark | Luồng đầy đủ → số đo chất lượng, batch, API/UI gắn run/dataset/code. |

T07 phải có trước khi fit thí nghiệm T05/T06 dù số task lớn hơn: chia tập trước khi tổng hợp dữ liệu học. API/UI có thể viết bằng fixture và tích hợp snapshot popularity trước ALS.

### 3.1. Thu thập catalog và sinh Bronze (T03)

Chạy từ gốc repo sau khi cài extras `pipeline,dev`. Lệnh đầu ưu tiên seed local;
nếu chưa có hoặc không đủ sản phẩm thì tải từ [Best Buy Open Data Set](https://github.com/BestBuyAPIs/open-data-set).
`--source` chọn tường minh file JSON list hoặc URL HTTP(S), kể cả khi đã có cache.

```powershell
.\.venv\Scripts\python -m recommender.ingestion.catalog --limit 1000 --output data/bronze/products/products.jsonl --seed 42
.\.venv\Scripts\python -m recommender.ingestion.synthetic --catalog data/bronze/products/products.jsonl --output data/bronze/events --events 100000 --users 1000 --sessions 5000 --seed 42
```

Catalog lưu seed JSON list đã chuẩn hóa tại `data/raw/products_bestbuy_1000.json`
để dùng offline. Bộ lọc loại SKU trùng, giá không dương/không hữu hạn, tên/ảnh
không hợp lệ, dịch vụ và danh mục ngoài công nghệ. Ba nhóm hỗn hợp Connected
Home, Musical Instruments và Toys chỉ nhận danh mục con điện tử. Chọn cân bằng
12 nhóm, ưu tiên bản ghi đầy đủ mô tả/thương hiệu/model/link, tie-break theo seed.
Nếu nguồn không đủ số lượng yêu cầu, CLI trả lỗi thay vì xuất thiếu sản phẩm.

Schema `Product` v1 được giữ nguyên: `category_id` là mã ngành hàng chữ thường,
`collected_at` là mốc snapshot thí nghiệm `2026-10-01T00:00:00Z`; `attributes`
lưu `price`, tên `category`, `model`, `is_active="true"` và `tags` là chuỗi JSON
list (đọc bằng `json.loads`). Đây là metadata trong object phẳng string → string
theo hợp đồng. `image_url` và `product_url` được nâng sang HTTPS.

Synthetic nhận `--output` là thư mục hoặc file `.jsonl`. Mặc định sinh 100.000
event; nếu chỉ giảm `--events`, CLI tự chọn số user/phiên phù hợp. Khi truyền
tường minh cần `1 <= users <= sessions <= events`; dataset rỗng dùng cả ba số 0.
Mỗi user có sở thích danh mục bền vững (85% chọn trong nhóm yêu thích), sản phẩm
được lấy theo Zipf (số mũ 1,2), phiên có chuỗi view → click → add_to_cart và
giỏ hai món lặp lại. Giỏ thể hiện đồng xuất hiện, không khẳng định tương thích
kỹ thuật. `cart_id` luôn null với view/click. Event được stream trực tiếp ra file,
không giữ toàn bộ sự kiện trong RAM.

Thời gian mô phỏng trải từ `2026-10-02` đến `2026-11-08` UTC để phủ các cutoff
thí nghiệm; nếu catalog có snapshot mới hơn, cửa sổ tự dịch sau snapshot đó.
Cùng catalog, tham số, seed và phiên bản Faker/NumPy tạo cùng byte JSONL. Manifest
ghi phiên bản thư viện và thời gian công bố thực, cùng đầy đủ thống kê, phiên bản
catalog, commit/code_dirty và SHA-256 tính từ byte dữ liệu. Mỗi thư mục đầu ra có
`manifest.json` tương ứng file vừa công bố; dùng thư mục riêng cho mỗi gói cần giữ.

```powershell
.\.venv\Scripts\python -m recommender.ingestion.catalog --source data/raw/products_bestbuy_1000.json --limit 1000 --seed 42
.\.venv\Scripts\python -m pytest tests/test_ingestion.py -v
.\.venv\Scripts\python -m pytest tests/ -v
.\.venv\Scripts\python -m ruff check src tests
```

### 3.2. Profiles tiếng Việt và luồng Kafka tùy chọn

Sinh profiles và events theo đúng user_id, sở thích chính/phụ:

```powershell
.\.venv\Scripts\python -m recommender.ingestion.users --limit 1000 --seed 42
.\.venv\Scripts\python -m recommender.ingestion.synthetic --catalog data/bronze/products/products.jsonl --users-file data/bronze/users/users.jsonl --events 100000 --sessions 5000 --output data/bronze/events/profile-demo --seed 42
.\.venv\Scripts\python -m recommender.streaming.producer --dry-run --max-events 20 --rate 100 --seed 42
```

Users CLI mặc định ghi seed `data/raw/users_1000.json`, JSONL
`data/bronze/users/users.jsonl` và manifest cùng thư mục JSONL. Có thể đổi bằng
`--cache-path`/`--output`. Loader hỗ trợ cả seed array và JSONL, từ chối ID trùng
và category không có trong catalog. `--users-file` dùng toàn bộ profiles, thay cho
`--users`; số events phải đủ để mỗi user có ít nhất một phiên. Sở thích phụ chiếm
25% lựa chọn có thiên hướng trong các funnel ngẫu nhiên; giỏ mẫu lặp ưu tiên chính.

Khởi động Docker Engine rồi chạy từ gốc repo:

```powershell
docker compose config --quiet
docker compose up -d
docker compose ps
# Terminal 1: 100 events hoặc 5 giây mỗi batch, Ctrl+C flush phần còn dư.
.\.venv\Scripts\python -m recommender.streaming.consumer --max-events 1000
# Terminal 2:
.\.venv\Scripts\python -m recommender.streaming.producer --rate 20 --max-events 1000 --seed 42
```

PostgreSQL dùng `recommender/change-me`, database `recommender`, cổng 5432.
MinIO API/Console ở cổng 9000/9001, `minioadmin/minioadmin`; `minio-init` đợi dịch
vụ và tạo ba bucket bronze/silver/gold theo cách idempotent với
[`mc mb --ignore-existing`](https://docs.min.io/aistor/reference/cli/mc-mb/).
Kafka dùng [cấu hình KRaft của image Apache 3.8.0](https://github.com/apache/kafka/blob/3.8.0/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml),
client trên host kết nối `localhost:9092`, trong network Docker dùng
`kafka:19092`. Healthcheck tạo sẵn topic `product-events`; auto-create cũng bật.
Ba dịch vụ lưu dữ liệu trong named volumes riêng trên network bridge.

Producer hỗ trợ `--catalog`, `--users`, `--bootstrap-servers`, `--topic`, `--rate`,
`--max-events`, `--seed`, `--dry-run`. Không truyền max-events thì chạy tới Ctrl+C.
Dry-run chỉ in event JSONL ra stdout; thống kê ra stderr và không kết nối broker.
Timestamp live dùng giờ UTC thực, còn seed cố định lựa chọn hành vi; mỗi lần chạy
có namespace ID riêng để tránh conflict khi khởi động lại.

Consumer hỗ trợ `--bootstrap-servers`, `--topic`, `--group-id`, `--output-dir`,
`--max-events`, thêm `--batch-size` (100), `--flush-interval` (5 giây) và
`--local-only`. Mỗi batch có JSONL/manifest tại
`data/bronze/events/streaming/batch_<uuid>/` và được upload đến
`bronze/events/streaming/batch_<uuid>/`. Consumer luôn lưu local trước; MinIO lỗi
thì giữ batch local và ghi cảnh báo. Khi MinIO hoạt động lại, có thể đồng bộ các
thư mục batch còn thiếu bằng `mc mirror data/bronze/events/streaming local/bronze/events/streaming`.
Manifest là dấu hiệu batch hoàn chỉnh, chỉ đọc các batch có manifest và checksum đúng.
Offset Kafka được commit sau khi lưu, chỉ tới record thực sự đã xử lý; replay
at-least-once nên ETL vẫn phải dedup event_id. `group-id` mới đọc lại từ đầu topic.

Kiểm thử users, dry-run, mock Kafka producer/consumer và MinIO hoàn toàn offline:

```powershell
.\.venv\Scripts\python -m pytest tests/test_users.py tests/test_streaming.py -v
```

### 3.3. ETL, mô hình, đánh giá và Gold snapshot (T04–T07)

Chạy từ gốc repo sau bước 3.1. `DATA_LAKE_STORAGE=local` ghi vào `DATA_LAKE_DIR` (mặc định `data/`); `minio` ghi `s3a://bronze|silver|gold/...` qua `hadoop-aws` (Spark tự tải package, cần MinIO đang chạy). Khi `.env` chép từ `.env.example` (giá trị `minio`) mà chưa có MinIO, đặt `DATA_LAKE_STORAGE=local` trong `.env`.

```powershell
# T04: Bronze JSONL -> Silver Parquet + quarantine + quality report (no-op nếu đã có cùng dataset_version)
.\.venv\Scripts\python -m recommender.pipeline.etl
# T07 + Gold: chọn cấu hình trên validation, chấm test một lần, rồi fit lại và xuất snapshot phục vụ
.\.venv\Scripts\python -m recommender.pipeline.snapshot
# Biến thể: chỉ benchmark | grid rút gọn để thử nhanh | bỏ benchmark, dùng cấu hình mặc định
.\.venv\Scripts\python -m recommender.evaluation.benchmark
.\.venv\Scripts\python -m recommender.pipeline.snapshot --quick
.\.venv\Scripts\python -m recommender.pipeline.snapshot --skip-evaluation
.\.venv\Scripts\python -m pytest tests/test_etl.py tests/test_ranking_tfidf.py tests/test_ranking_advanced.py tests/test_evaluation.py tests/test_snapshot.py -v
```

| Bước | Module | Đầu ra |
| --- | --- | --- |
| ETL | `pipeline/etl.py` | `silver/products/<v>/`, `silver/events/<v>/`, `silver/quarantine/<v>/`, `silver/quality/<v>.json`, `silver/<v>.manifest.json` (ghi sau cùng). `<v>` = `ds-` + SHA-256 của checksum file Bronze và `ETL_VERSION`. Conflict cùng ID khác payload chặn phát hành (exit code 3, không có manifest). |
| Gold base | `pipeline/gold.py` | `catalog_as_of`, `interactions` (1/2/4), `baskets` (chỉ `add_to_cart`), `seen_products` cho từng cửa sổ `[start, end)`. |
| Split | `evaluation/split.py` | Hai cutoff `2026-10-25`, `2026-11-01` UTC; purge giỏ đi qua cutoff; kiểm tra bất biến chống rò rỉ. |
| Mô hình | `ranking/` | `popularity`, `tfidf` (CountVectorizer + IDF), `substitutes`, `als` (`implicitPrefs=True`), `mmr`, `fpgrowth` (metric tính lại theo từng consequent). |
| Benchmark | `evaluation/benchmark.py` | `gold/<run_id>/evaluation/`: `config.json`, `split_summary.json`, `selection.json`, `quality.csv`, `complements.csv`, `batch_timings.csv`. |
| Snapshot | `pipeline/snapshot.py` | `gold/<run_id>/` gồm `products`, `interactions`, `baskets`, `seen_products`, `recommendation_items`, `association_rules`, `models/*` và `manifest.json` (fingerprint = SHA-256 của manifest; run đã có thì không ghi đè). |

TF-IDF fit bằng Spark ML trên catalog as-of cutoff; ma trận vector (1.000 × ~5.000) được chuẩn hóa L2 ở driver bằng NumPy vì chiều sản phẩm nhỏ, còn chiều sự kiện (interactions, split, ALS, FP-Growth) chạy trên Spark. ALS xuất top-200 ứng viên/user sau lọc seen và catalog; MMR tái xếp trên 200 ứng viên đó, thứ tự hạng là thứ tự chọn.

**Kết quả đo 17/11/2026** trên dữ liệu chung của nhóm: catalog `data/bronze/products/` và 1.000 profiles `data/bronze/users/` từ `main`, cộng 100.000 events sinh theo profile với seed 42, theo lệnh ở mục 3.2 (`--output data/bronze/events/profile-demo`). Silver `ds-146a72bc3421240b42c94a87`. Run `run_20261108T000000Z_alsmmr_42_26faa7fc` được tạo từ commit sạch. Cấu hình máy: Spark `local[2]`, driver 2 GB, Linux VM 4 vCPU/3 GB, JDK 11; benchmark đủ grid cộng snapshot mất 1 phút 35 giây. Split: train 62.151 / validation 18.671 / test 19.175 events, purge 1 giỏ (3 events), bốn bất biến đều đạt. Cấu hình chọn trên validation: ALS `rank=16, regParam=0.1, alpha=1, maxIter=10`; MMR `lambda=1.0`; FP-Growth `minSupport=0.001, minConfidence=0.5`.

| Test (508 user, 1.782 nhãn) | Recall@10 | NDCG@10 | ILD | Coverage | Fallback |
| --- | --- | --- | --- | --- | --- |
| Popularity | 0,0838 | 0,0552 | 0,9898 | 0,0240 | 100% |
| TF-IDF cá nhân hóa | 0,0531 | 0,0358 | 0,7507 | 0,2870 | 0% |
| Implicit ALS | 0,2764 | 0,2124 | 0,9518 | 0,2890 | 0% |
| ALS + MMR (`lambda=1.0`) | 0,2764 | 0,2124 | 0,9518 | 0,2890 | 0% |

Trên validation, MMR đổi độ phù hợp lấy đa dạng: `lambda` 0,3 / 0,5 / 0,7 / 1,0 cho NDCG@10 0,2036 / 0,2082 / 0,2116 / 0,2140 và ILD 0,976 / 0,969 / 0,962 / 0,952. Tiêu chí chọn theo NDCG nên `lambda=1.0` thắng, ALS + MMR trùng ALS ở test. TF-IDF cá nhân hóa thấp hơn popularity ở dữ liệu theo profile: người dùng có thêm sở thích phụ (25% lựa chọn), trong khi hồ sơ nội dung kéo gợi ý về danh mục đã xem nhiều nhất. Mua kèm FP-Growth: Hit@10 của item giữ lại = 0,806 trên 935 giỏ test, 485 luật. Con số này cao vì bộ sinh cố ý lặp giỏ hai món theo danh mục; không suy rộng sang người mua thật. Tất cả user đánh giá đều có lịch sử train, nên cohort `new_user` ở trạng thái `not_evaluable`.

ETL chỉ đọc file JSONL được `manifest.json` cùng thư mục liệt kê, với SHA-256 và kích thước khớp. Điều này áp dụng cho cả micro-batch Kafka trong `bronze/events/streaming/batch_<uuid>/`. File chưa có manifest hoặc bị sửa sau khi công bố được bỏ qua và ghi vào `bronze_files_skipped` của quality report. Event live của producer mang giờ UTC thực, nên nằm ngoài cửa sổ split 01/10–08/11 của thực nghiệm và được đếm vào `events_outside_window`; snapshot phục vụ vẫn dùng chúng vì cửa sổ train mặc định lấy từ min đến max `event_at`.

## 4. Kịch bản nghiệm thu demo

1. Ghi dataset version, run ID, seed và commit. Chọn snapshot ready; kiểm tra catalog/kết quả cùng run. Health kiểm tra tiến trình, catalog kiểm tra DB/snapshot.
2. User có lịch sử nhận Dành cho bạn đã lọc seen; user mới nhận popularity có thứ tự ổn định. Danh sách thiếu ứng viên có thể ngắn/rỗng.
3. Trang chi tiết chỉ trả substitutes cùng danh mục, không chứa sản phẩm gốc. Giỏ trả complements theo confidence/lift, loại item đã có; giỏ rỗng không có mua kèm.
4. Mỗi thao tác có UUID cố định khi retry. Gửi lại cùng UUID/payload trả 200, không tăng số dòng; payload khác cùng UUID trả 409 và giữ bản gốc.
5. Giữ batch cố định, click danh mục khác trong cùng phiên; đợi POST 201/200 rồi GET có session. Ghi top 10 trước/sau cùng run, dùng fixture có tín hiệu rõ. Hai phiên đồng thời phải tách lịch sử.
6. Xuất log demo → Bronze → ETL/batch mới → import. Event xuất lại không được cộng weight hai lần; snapshot mới chỉ xuất hiện sau commit.
7. Import thiếu bảng/sai checksum giữ active run cũ. Rollback có chủ đích sang run ready trước phải khôi phục cả catalog và gợi ý.

UI dùng status code từ [hợp đồng API](DATA_CONTRACTS.md), không hiển thị trace/chuỗi kết nối. Nếu chưa có snapshot, báo dữ liệu chưa sẵn sàng. Không lấy fixture ngầm để giả lập dịch vụ hoạt động.

## 5. Kiểm tra và bằng chứng bàn giao

T01 kiểm tra file tồn tại, link relative kể cả anchor, fence Markdown, JSON ví dụ parse được, nội dung không bỏ dở và sự thống nhất field/enum/weight. Không dùng pytest rỗng làm bằng chứng tài liệu.

Từ T02, chạy test liên quan theo PLAN. Khi đã có test, các lệnh chung từ gốc:

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m pytest
```

Pytest chưa có test trả exit code 5, không phải pass. End-to-end được nghiệm thu sau T12 trên cả hai máy từ checkout sạch. Lưu lệnh thực chạy, cấu hình không có secret, output kiểm tra và run/dataset dùng. [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md) quy định raw output, công thức, warm-up, số lượt và percentile.

## 6. Xử lý lỗi vận hành

| Triệu chứng | Kiểm tra và khôi phục |
| --- | --- |
| Cài package báo sai Python | Dùng `py -3.11` và interpreter `.venv`; package yêu cầu `>=3.11,<3.12`. |
| Spark không khởi động | Kiểm tra JDK/JAVA_HOME/PATH, quyền ghi temp và smoke check trước dữ liệu lớn. |
| Không kết nối PostgreSQL | Kiểm tra service, host/port/database/role, mật khẩu URL-encode; dùng psql kiểm tra độc lập. |
| Health 200, catalog 503 | Health là liveness; kiểm tra DB và active_run_id. |
| Dataset sai hash | Giữ bản đã xác minh, tải lại vào vị trí tạm, kiểm tra đủ manifest. |
| Import lỗi | Xem lỗi run; retry cùng fingerprint hoặc tạo run mới, không sửa con trỏ bằng tay. |
| User/giỏ không có gợi ý | Kiểm tra catalog/seen/rules; rỗng có thể hợp lệ, không tự bỏ bộ lọc. |
| Retry event trả 409 | Giữ payload gốc cho cùng UUID; UUID mới chỉ cho thao tác mới. |
| Click chưa ảnh hưởng session | Kiểm tra user/session, commit, cửa sổ 30 phút, substitute và run chưa đổi. |

Nguồn catalog/kho chia sẻ được nghiệm thu ở P0/P2/P5. Giữ bản local đã xác minh để demo khi mất mạng; báo cáo luôn phân biệt dữ liệu mô phỏng và demo.
