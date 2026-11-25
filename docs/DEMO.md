# Chạy và bàn giao phần ứng dụng

Cập nhật 25/11/2026. Chạy từ gốc repo bằng Python 3.11. Phần serving đã chạy với fixture; ETL, TF-IDF/ALS/MMR và đánh giá trên dữ liệu thật của Pháp chưa được tích hợp.

## 1. Chuẩn bị

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[pipeline,app,dev]"
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up -d
docker compose ps
```

Docker Desktop cần hoạt động. `.env.example` dùng PostgreSQL 5432; trên máy Thành có PostgreSQL Windows chiếm cổng này, nên `.env` local đặt `POSTGRES_PORT=5433` và DATABASE_URL dùng 5433. Hai giá trị phải cùng cổng. Các cổng khác: Kafka 9092, MinIO API 9000/Console 9001, API 8000, UI 8501. Các image hiện có trong compose phục vụ môi trường phát triển local.

## 2. Chạy ngay với fixture có nhãn

Fixture có **5 sản phẩm**, user `u1`, luật A → D và similarity A → C. Đây là dữ liệu dựng để kiểm thử đường phục vụ, không phải output huấn luyện từ 1.000 sản phẩm. Lệnh tạo không ghi đè run cũ; đổi `--run-id` nếu đã tồn tại.

```powershell
.\.venv\Scripts\python.exe -m recommender.pipeline.demo_fixture --output data/gold --run-id fixture_demo
.\.venv\Scripts\python.exe scripts/run_demo.py --snapshot-dir data/gold/fixture_demo
```

Runner migrate, validate checksum/schema, import và bật API. Terminal thứ hai:

```powershell
.\.venv\Scripts\python.exe -m streamlit run src/recommender/ui/app.py --server.address=127.0.0.1 --server.port=8501 --browser.gatherUsageStats=false
Invoke-RestMethod http://127.0.0.1:8000/health
```

Mở [website demo](http://127.0.0.1:8501) và [OpenAPI](http://127.0.0.1:8000/docs). `/health` trả `{"status":"ok"}` kể cả khi DB ngắt; `/api/v1/products` kiểm tra cả DB/snapshot. Dừng từng terminal bằng Ctrl+C.

## 3. Kịch bản đã kiểm tra

1. Chọn user `u1`, tắt cập nhật phiên: Dành cho bạn bắt đầu B, C, rồi fallback D/E; A đã seen.
2. Bật cập nhật theo phiên, mở Product A. UI gửi một click và một view với hai UUID. Gợi ý cá nhân hóa chuyển C lên trước B trong cùng `fixture_demo`; batch không train lại.
3. Chọn tab Đang xem: Product A, ảnh fallback và Sản phẩm thay thế C.
4. Thêm A vào giỏ, sang Giỏ hàng: có A và Thường thêm cùng D. Xem [ảnh browser thật](demo/cart-fixture.jpg).
5. Đổi user thành mã mới: tạo session/cart mới, nhận popularity. Đổi toggle/tab hoặc rerun không sinh thêm event.
6. Khi API ngắt, event chưa nhận được giữ trong state; bấm Gửi lại sự kiện với cùng UUID. Session/cart của payload cũ vẫn được giữ kể cả sau đổi user. Event bị từ chối vĩnh viễn (404/409/422) được giữ riêng kèm lý do và không chặn các thao tác mới. Xóa giỏ tạo cart_id mới trong cùng session, giữ nguyên ID của payload đã xếp hàng.

API không có xác thực người dùng ở phạm vi demo. User ID là mã mô phỏng tự chọn. UI chỉ gọi HTTP; không train hay đọc DB trực tiếp.

## 4. Nhập Gold của Pháp và rollback

Pháp bàn giao thư mục `<run_id>/manifest.json` cùng toàn bộ file được liệt kê. Bốn bảng Parquet bắt buộc: products, recommendation_items, seen_products, association_rules. Bảng rules rỗng vẫn có schema. Xem [hợp đồng](DATA_CONTRACTS.md) và [migration/ERD](DATABASE.md).

```powershell
.\.venv\Scripts\python.exe -m recommender.api.migrate
.\.venv\Scripts\python.exe -m recommender.pipeline.importer --snapshot-dir data/gold/<run_id>
# Khôi phục run đã ready, giữ cả catalog và gợi ý của run đó:
.\.venv\Scripts\python.exe -m recommender.pipeline.importer --activate-run <run_id_cu>
```

Importer kiểm tra byte/checksum mọi file và nội dung bốn bảng serving. Pháp kiểm tra model/vocabulary/ALS mappings ở bước phát hành Gold. Cùng run ID khác fingerprint bị từ chối. Retry run ready không tự kích hoạt lại run cũ. Request đang đọc tiếp tục dùng một run nhất quán.

Nếu chạy runner với run ready cũ trong khi run khác active, runner dừng và chỉ rõ lệnh `--activate-run` cần dùng; runner không tự rollback. UI lấy metadata gợi ý với `X-Expected-Run-ID`, tải lại nhóm khi snapshot đổi và không ghép tên/danh mục của run khác.

Luật FP-Growth của Thành nhận baskets cùng run, loại giỏ dưới hai item khỏi fit. Đưa các file rules cùng checksum vào manifest Gold do Pháp phát hành:

```powershell
.\.venv\Scripts\python.exe -m recommender.ranking.fpgrowth --baskets data/gold/<run_id>/baskets --output data/models/<run_id>-fpgrowth --min-support 0.01 --min-confidence 0.1
```

Các ngưỡng trên là mặc định runner, chưa phải cấu hình được chọn qua validation. Đã kiểm tra Spark fit → ghi Parquet bằng PyArrow → Gold validator với 4 luật. Native Spark model save và toàn bộ CLI I/O chưa được nghiệm thu trên Windows; log Spark máy Thành hiện báo thiếu winutils/HADOOP_HOME, cần kiểm tra trong môi trường train trước khi dùng lệnh trên cho báo cáo. Compatibility là mapping tường minh cặp category → thuộc tính; mặc định API chỉ diễn giải mua kèm là đồng xuất hiện trong giỏ. Có thể gắn mapping vào `app.state.compatibility` trước khi serve.

## 5. Xuất event và chia sẻ MinIO

```powershell
.\.venv\Scripts\python.exe -m recommender.pipeline.export_events --output-dir data/bronze/demo-events
.\.venv\Scripts\python.exe -m recommender.ingestion.sync_minio --source-dir data/bronze/demo-events
# Dùng đúng prefix in ra từ lệnh upload:
.\.venv\Scripts\python.exe -m recommender.ingestion.sync_minio --download-prefix datasets/<dataset_version>/<manifest_hash> --output-dir data/downloads/<dataset_version>
```

Export quét toàn bộ event demo nhìn thấy trong một transaction đọc nhất quán, không dùng watermark để bỏ qua commit muộn. File chỉ có tám trường Event; metadata `exported_at`/`received_at_max` nằm trong manifest. Xuất lại giữ UUID, ETL phải dedup trước cộng trọng số. Không xóa log DB trong exporter.

Hai máy phải cấu hình cùng MINIO_ENDPOINT là địa chỉ LAN/VPN của máy chủ, cùng bucket và tài khoản có quyền tương ứng; `127.0.0.1` trên máy Pháp trỏ tới máy Pháp. Máy Thành đã kiểm tra upload/download catalog và users 1.000 dòng, 2 batch 8 events đúng SHA. **Mạng và tài khoản riêng trên máy Pháp chưa được nghiệm thu.** Hướng dẫn vận hành MinIO chi tiết ở [README](README.md).

## 6. Kiểm thử và benchmark

Tạo riêng database `recommender_test` với owner của dự án. URL dưới đây là mẫu môi trường local; sửa cổng theo `.env`. Fixture DB chỉ chạy khi TEST_DATABASE_URL được đặt tường minh và tên DB kết thúc `_test`. Fixture truncate bảy bảng trong DB test này giữa các ca.

```powershell
$env:TEST_DATABASE_URL='postgresql://recommender:change-me@127.0.0.1:5433/recommender_test'
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check src tests scripts
```

Benchmark ghi event, nên chạy API với DATABASE_URL của **database benchmark riêng** đã migrate/import, ví dụ `recommender_benchmark`. Không hướng nó vào DB demo báo cáo. Sau warm-up:

```powershell
.\.venv\Scripts\python.exe scripts/benchmark_api.py --base-url http://127.0.0.1:18010 --requests 100 --concurrency 4 --output docs/results/api-benchmark.json
```

[Số đo đã chạy](results/api-fixture-benchmark.json) lưu từng mẫu độ trễ, status, cấu hình máy, run và fingerprint; [báo cáo phần Thành](THANH_REPORT.md) giải thích giới hạn. Lượt này chỉ đo serving fixture 5 sản phẩm; chưa đo chất lượng model, tốc độ ETL/train hay tải trên catalog 1.000 sản phẩm.

## 7. Các bước còn chờ phối hợp

- Pháp bàn giao ETL/Silver và Gold TF-IDF/ALS/MMR đã chọn cấu hình qua validation, cùng artifact/manifest/metric.
- Nối output FP-Growth vào publisher Gold, chạy export → ETL dedup → train → import run mới với dữ liệu thật.
- Kiểm tra trên máy Pháp từ checkout sạch và endpoint MinIO dùng chung; cùng gọi request trên cùng snapshot.
- Đo lại API với Gold thật, đối chiếu metric và hoàn thành báo cáo/slide chung. Không lấy fixture thay cho kết quả thực nghiệm.
