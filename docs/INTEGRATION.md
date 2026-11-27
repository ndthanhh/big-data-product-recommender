# Tích hợp PR #1 với phần serving

Nhánh: `codex/integrate-ml-pipeline`. Hai đầu vào: Thành `767c97a` và Pháp `543b001` ([PR #1](https://github.com/ndthanhh/big-data-product-recommender/pull/1)). Nhánh tích hợp giữ lịch sử cả hai bên; chưa đưa vào `main`.

## Các điểm ghép

| Thành phần | Sau tích hợp |
| --- | --- |
| Kiểm tra Gold | `pipeline/snapshot.py`: `validate_snapshot`, dùng bởi importer. |
| Train và xuất Gold | `pipeline/publish_snapshot.py`: `make_run_id`, `build_snapshot`, CLI riêng. |
| FP-Growth | Lõi `fit_fpgrowth` của pipeline; `fit_rules` và CLI gọi lại cùng lõi. Giỏ dedup trước fit, loại giỏ dưới hai item; native model ở `models/fpgrowth`. |
| Test fixtures | Một Spark session cho suite; DB test được chọn tường minh, tên kết thúc `_test`. Test PowerShell chạy trên Windows. |
| Bronze → ETL | Đọc `products/`, `events/`, `demo-events/` và `datasets/<version>/<hash>/`; kiểm manifest/checksum, bỏ staging ẩn. |

ETL `1.2.0` giữ metadata của từng manifest thực sự đóng góp dữ liệu. `parameters.bronze_dataset_versions` là danh sách ID theo bảng; `generator_versions`, `seeds` và tham số batch được giữ khi nguồn hỗn hợp. Catalog version tổng hợp theo các input đã phân loại, không phụ thuộc đường dẫn có bắt đầu bằng `products/` hay không.

Run ID chứa cả hai mốc train và cấu hình đánh giá (grid/split/chính sách), bỏ thời điểm đo và thông tin máy khỏi phần định danh cấu hình. Run đã công bố chỉ được no-op khi các input khai báo khớp; đổi cửa sổ hoặc cấu hình cần run mới. Publisher của demo chạy tuần tự cho cùng run; chưa có khóa phân tán giữa hai publisher cùng run. Importer vẫn có khóa singleton và kiểm tra tranh chấp như trước.

## Chạy Linux trên máy Windows

Spark train và native model/Parquet I/O được kiểm tra trên Linux để tránh phụ thuộc winutils của Windows. Image kiểm thử cài Python 3.11, JDK 17, PySpark **3.5.7** và Git. Giới hạn BLAS một thread; Spark chạy `local[2]`.

```powershell
docker build -f scripts/Dockerfile.test -t big-data-tests:integration .
$repo=(Get-Location).Path
# Đổi cổng/password nếu cấu hình PostgreSQL local khác. Chỉ dùng DB test riêng.
docker run --rm --memory=5g --shm-size=1g --mount "type=bind,source=$repo,target=/workspace,readonly" -e TEST_DATABASE_URL=postgresql://recommender:change-me@host.docker.internal:5433/recommender_test big-data-tests:integration
```

`.dockerignore` loại `.env`, `.git`, virtualenv và dữ liệu khỏi build context. Khi test, repo được bind read-only; fixture ghi vào `/tmp`, DB dùng database test riêng. Git trong container chỉ tin đường dẫn `/workspace` và không refresh index.

Để chạy pipeline trên một Data Lake local riêng và ghi kết quả về máy Windows:

```powershell
$lake=(Resolve-Path data/integration/real-lake).Path
docker run --rm --mount "type=bind,source=$repo,target=/workspace,readonly" --mount "type=bind,source=$lake,target=/lake" -e DATA_LAKE_DIR=/lake big-data-tests:integration python3 -m recommender.pipeline.etl
docker run --rm --mount "type=bind,source=$repo,target=/workspace,readonly" --mount "type=bind,source=$lake,target=/lake" -e DATA_LAKE_DIR=/lake big-data-tests:integration python3 -m recommender.pipeline.publish_snapshot --skip-evaluation --driver-memory 1g
```

Gói `real-lake` phải có Bronze products/events đã công bố. `--skip-evaluation` là kiểm tra đường chạy, không chọn hyperparameter hay công bố metric mới. Đánh giá đầy đủ dùng publisher không có cờ này hoặc theo lệnh ở [README](README.md#33-etl-mô-hình-đánh-giá-và-gold-snapshot-t04t07).

## Bàn giao MinIO về máy train

Dùng đúng prefix do sync in ra. Với một gói catalog:

```powershell
python -m recommender.ingestion.sync_minio --download-prefix datasets/<dataset_version>/<hash> --output-dir data/bronze/products/shared/catalog-v1
```

Gói events tải vào `data/bronze/events/shared/<version>`, gói users vào `data/bronze/users/shared/<version>`. ETL học tương tác từ products/events; profiles được bộ sinh sử dụng. Không tải catalog vào `bronze/shared` vì thư mục này không thuộc các layout được ETL đọc. Endpoint LAN/tài khoản trên máy Pháp vẫn cần kiểm tra thực tế.

## Review và nghiệm thu

Review độc lập phát hiện bốn vấn đề đã được đưa vào regression tests: provenance của sync batches, định danh/caching sai cửa sổ train/grid, thiếu native FP-Growth model và đường dẫn download không được ETL đọc. Các test quan sát fail trước sửa; native FP-Growth được kiểm tra bằng `FPGrowthModel.load`, cùng checksum và retry publisher.

Kết quả cuối và dữ liệu nghiệm thu nằm trong [nhật ký tích hợp](superpowers/plans/2026-11-27-integrate-ml-pipeline.md). Các số liệu 17/11 của Pháp và benchmark fixture 25/11 của Thành là bằng chứng lịch sử riêng, không thay cho kết quả của nhánh tích hợp. Dataset/model lớn không được commit vào Git.

Khi nhánh đã được push và các kiểm tra đạt, mở PR từ nhánh tích hợp vào `main`. PR này chứa cả code của PR #1 và phần serving, nên kiểm tra bản kết hợp trước khi merge vào main.

## Kết quả 27/11/2026

- Linux: 487 pass, 3 test PowerShell skip; Windows: 463 test ngoài nhóm Spark pass trước review, 16 regression/PowerShell pass sau sửa. Ruff và format79 file pass.
- Luồng fixture thật train→Gold→validator→PostgreSQL→3 nhóm API→event→export hai lần→ETL khử trùng đã pass. Native ALS và FP-Growth model tải lại được.
- Lượt 100.000 events/1.000 sản phẩm sinh 200.000 personalized rows, 82.336 substitutes, 746 association rules; API user cũ/mới, substitutes, complements, session và event 201→200 đều đạt. DB nghiệm thu là recommender_integration_test, không đổi DB demo.
- [JSON kết quả dữ liệu lớn](results/integration-real-handoff.json) và [verification](results/integration-verification.json) giữ run/dataset/fingerprint/config. Model được tạo trong working tree đang merge nên code_dirty=true; không gán cho commit sạch hoặc coi đây là metric đánh giá mới. Các thời gian được đo khi chạy test đồng thời và không dùng làm benchmark tải.
