# Hướng dẫn vận hành và tích hợp

[Trang chủ repo](../README.md) · [Tech Stack](TECH_STACK.md) · [Quy ước đồng nhất](CONVENTIONS.md) · [Kiến trúc](ARCHITECTURE.md) · [Hợp đồng v1](DATA_CONTRACTS.md) · [Thực nghiệm](EXPERIMENT_PLAN.md) · [Kế hoạch & Phân công](../PLAN.md) · [Checklist](PROJECT_CHECKLIST.md)

## 1. Trạng thái có thể chạy sau T01

Repo hiện có `pyproject.toml`, `.env.example`, cấu trúc `src/recommender/`, các lớp `data/` và thư mục `tests/fixtures/`. Các module mới có `__init__.py`. T01 hoàn thiện tài liệu; chưa có entry point ETL, train, importer, FastAPI hoặc Streamlit. Fixture và test hợp đồng thuộc T02. Cài package thành công chưa phải bằng chứng hệ thống chạy end-to-end.

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
