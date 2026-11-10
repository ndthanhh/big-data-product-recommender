# Quy ước làm việc và phân công Nhóm 1

Dự án **Hệ thống gợi ý sản phẩm dựa trên hành vi người dùng** do Nguyễn Đức Thành và Phan Văn Pháp thực hiện. File này là thỏa thuận v0.1 để hai người bắt đầu song song. Mọi thay đổi về schema, API hoặc tiêu chí bàn giao được cập nhật tại đây và có review của người còn lại.

## 1. Ai phụ trách phần nào

| Người | Phụ trách chính | Bàn giao cho người còn lại |
| --- | --- | --- |
| **Nguyễn Đức Thành** | Thu thập catalog từ nguồn cho phép, thiết kế ba lớp Data Lake, ETL PySpark, popularity, TF-IDF, Implicit ALS, MMR và đánh giá chất lượng gợi ý. | Catalog và sự kiện sạch, ánh xạ ID cho ALS, tập ứng viên và snapshot phục vụ, bảng so sánh mô hình và thời gian batch. |
| **Phan Văn Pháp** | Sinh hành vi bằng Faker/NumPy, lưu và xuất log tương tác, PostgreSQL, FastAPI, Streamlit, FP-Growth và lọc tương thích cho gợi ý mua kèm. | Mẫu log đúng schema, migration và bảng phục vụ, luật mua kèm từ giỏ sạch, API/demo và số đo phản hồi API. |
| **Cả hai** | Chốt nguồn catalog, review PR cho nhau, tích hợp, tái lập thực nghiệm, viết và trình bày báo cáo. | Thành viết phần dữ liệu, mô hình và chất lượng. Pháp viết phần API, demo, hiệu năng API và mua kèm. Cùng duyệt phần tổng quan, giới hạn và slide. |

Thành sở hữu `pipeline/`, `evaluation/`, phần catalog trong `ingestion/` và các thuật toán xếp hạng ngoài mua kèm. Pháp sở hữu `api/`, `ui/`, phần sự kiện trong `ingestion/` và module mua kèm trong `ranking/`. Người phụ trách thay đổi nào mở issue và PR cho phần đó.

## 2. Môi trường và cách tổ chức code

- Dùng Python 3.11, JDK 17, PySpark 3.5.7 và PostgreSQL local. Chạy từ thư mục gốc. Tạo `.venv` và cài bằng `python -m pip install -e ".[pipeline,app,dev]"` sau khi kích hoạt môi trường. Khi bổ sung thư viện, cập nhật `pyproject.toml` trong cùng PR.
- Dùng UTF-8, LF, 4 dấu cách. Tên file, biến và hàm dùng `snake_case`, lớp dùng `PascalCase`, hằng số và biến môi trường dùng `UPPER_SNAKE_CASE`. Đặt type hint cho hàm công khai. Định dạng bằng Ruff. Chạy `python -m ruff check .`, `python -m ruff format --check .` và `python -m pytest` khi đã có test.
- Code dưới `src/recommender/`. Test đặt ở `tests/`, tên `test_*.py`. Ưu tiên test với fixture nhỏ có thể chạy offline. Module import được mà chưa khởi chạy Spark, kết nối DB hay thực hiện HTTP. Entry point khởi tạo các dịch vụ khi chạy.
- Cấu hình local lấy qua `DATA_LAKE_DIR`, `DATABASE_URL`, `API_BASE_URL` và `RANDOM_SEED` (mặc định `42`). Ưu tiên giá trị từ môi trường tiến trình trước `.env`. Script dùng đường dẫn tương đối với gốc repo hoặc đường dẫn từ cấu hình. Không hardcode đường dẫn máy, mật khẩu hay token.
- Commit code, migration, tài liệu và fixture tổng hợp nhỏ. Giữ `.env`, raw data, log thật, model và output batch ở local. Chỉ ghi ID demo giả danh. Lưu nguồn, thời điểm lấy catalog và phạm vi cho phép sử dụng để nhóm kiểm tra lại.

## 3. Hợp đồng dữ liệu v1

Mỗi bản ghi Bronze là một object JSON trên một dòng UTF-8. Silver dùng Parquet với schema rõ ràng. Thời gian được nhận và xuất theo ISO 8601 UTC, ví dụ `2026-11-10T04:00:00Z`. Giữ ID dạng chuỗi xuyên suốt các lớp và API. ALS dùng ánh xạ riêng sang số nguyên 32-bit, lưu kèm batch.

| Đối tượng | Trường bắt buộc | Trường bổ sung và quy tắc |
| --- | --- | --- |
| Sản phẩm | `product_id`, `name`, `category_id`, `source`, `collected_at` (chuỗi) | `description`, `brand`, `image_url`, `product_url` (chuỗi hoặc null), `attributes` (object chuỗi → chuỗi, mặc định `{}`). ID ổn định dạng `<source>:<id_goc>`. |
| Sự kiện | `event_id` (UUID), `user_id`, `session_id`, `product_id`, `event_type`, `event_at`, `source` | `event_type` là `view`, `click` hoặc `add_to_cart`. `source` là `demo` hoặc `synthetic`. `cart_id` bắt buộc với `add_to_cart`, có thể null với hai loại còn lại. Giữ nguyên `event_id` khi gửi lại. |
| Tương tác Gold | `user_id`, `product_id`, `interaction_weight` (số), `last_event_at` | Tổng trọng số theo cặp user và sản phẩm trong cửa sổ đầu vào. Mặc định `view=1`, `click=2`, `add_to_cart=4`. |
| Kết quả Gold | `run_id`, `kind`, `context_id`, `product_id`, `rank` (int), `score` (số hữu hạn), `method` | `kind` là `personalized`, `substitute` hoặc `popular`. `context_id` lần lượt là user ID, sản phẩm gốc hoặc `global`. Rank bắt đầu từ 1, mỗi sản phẩm xuất hiện tối đa một lần trong một nhóm kết quả. |

Gold lưu bảng `association_rules` gồm `run_id`, `antecedent_ids`, `consequent_id`, `support`, `confidence` và `lift`. FastAPI lọc luật theo tập sản phẩm đang có trong giỏ. Khi một sản phẩm khớp nhiều luật, lấy luật có confidence cao nhất, rồi lift cao nhất. Điểm bằng nhau thì sắp theo `product_id`. Snapshot phục vụ gồm catalog, kết quả top-K, luật mua kèm và các cặp user–sản phẩm đã tương tác để lọc cả ứng viên popularity.

Ví dụ event để cả hai viết fixture, bộ sinh và API theo cùng một dạng.

```json
{"event_id":"ed8f2fa8-9b81-43b6-8bd4-9b8115640866","user_id":"u_demo_001","session_id":"s_demo_001","cart_id":"c_demo_001","product_id":"fixture:p001","event_type":"add_to_cart","event_at":"2026-11-10T04:00:00Z","source":"demo"}
```

- Đường dẫn đã thống nhất gồm `data/bronze/products/`, `data/bronze/events/`, `data/silver/products/`, `data/silver/events/` và `data/gold/<run_id>/`. Bronze ghi theo batch và nguồn. Silver khử trùng sự kiện theo `event_id`, ghi số bản ghi bị loại theo lý do. Giỏ được nhóm theo `cart_id` duy nhất toàn cục và khử trùng sản phẩm trong giỏ.
- Mỗi lần chạy ghi `run_id`, mốc dữ liệu, seed, tham số và số dòng theo bảng. Cùng đầu vào, cấu hình và seed phải tái tạo cùng tập dữ liệu chuẩn hóa và cùng phân chia train/validation/test. Chỉ chuyển API sang snapshot mới sau khi đã kiểm tra và nạp đủ bảng bằng một transaction.

## 4. Hợp đồng API v1

FastAPI chạy local tại `http://127.0.0.1:8000`. Streamlit lấy base URL từ `API_BASE_URL`. Prefix API là `/api/v1`. UI gọi API cho cả đọc và ghi dữ liệu.

| Method và route | Đầu vào | Kết quả |
| --- | --- | --- |
| `GET /health` | Không có | `200` và `{"status":"ok"}` khi tiến trình API hoạt động. |
| `GET /api/v1/products` | `limit` mặc định 20, từ 1 đến 100. `offset` mặc định 0, không âm. | `200` và `{"items":[...]}` theo thứ tự `product_id`. |
| `GET /api/v1/products/{product_id}` | ID sản phẩm | `200` với object sản phẩm, hoặc `404` nếu ID không có. |
| `POST /api/v1/events` | Object sự kiện trong mục 3, `source="demo"` | `201` và `{"event_id":"..."}`. Gửi lại cùng ID và nội dung trả `200`. Cùng ID nhưng khác nội dung trả `409`. |
| `GET /api/v1/recommendations/personalized` | `user_id`, `limit` | Danh sách cá nhân hóa. Dự phòng bằng popularity cho user mới hoặc khi thiếu ứng viên. Loại item user đã tương tác trong snapshot. |
| `GET /api/v1/recommendations/substitutes` | `product_id`, `limit` | Danh sách cùng danh mục, loại sản phẩm gốc. ID không có trả `404`. |
| `POST /api/v1/recommendations/complements` | `{"product_ids":["fixture:p001"],"limit":10}` | Gợi ý từ luật giỏ cho tập sản phẩm đã gửi. Bỏ ID trùng và loại các sản phẩm đã có trong giỏ. Giỏ rỗng trả danh sách rỗng. ID không có trả `404`. |

- Gợi ý trả `200` với `{"run_id":"...","items":[...]}` khi đã có snapshot. Mỗi item có `product_id`, `rank`, `score`, `method`. UI lấy tên và ảnh từ catalog. `limit` mặc định 10, hợp lệ từ 1 đến 50. Các nhóm có thể trả ít hơn limit. Không đủ ứng viên thì `items` rỗng.
- Body hoặc tham số sai schema trả `422`. Gợi ý trả `503` khi chưa có snapshot. Với sự kiện hợp lệ về schema nhưng sản phẩm không tồn tại, trả `404`. UI giữ một `user_id` và `session_id` ổn định trong phiên, giữ `cart_id` đến khi tạo giỏ mới.
- Pháp định nghĩa kiểu dữ liệu, khóa và migration PostgreSQL cho `products`, `events` và các bảng phục vụ. Thành và Pháp review chung bước đồng bộ Gold. Test ít nhất trường hợp batch lỗi trước khi kích hoạt và hai lần gửi cùng event.

## 5. Git, review và tiêu chí hoàn tất

1. Mỗi issue có một người phụ trách, kết quả cần bàn giao và cách kiểm tra. Tách issue khi hai người có thể làm độc lập. Ghi phụ thuộc giữa các issue để người kia biết lúc nào dùng fixture hoặc dữ liệu thật.
2. Tạo nhánh từ `main` sau khi pull. Dùng `feat/<chu-de>`, `fix/<chu-de>`, `docs/<chu-de>` hoặc `chore/<chu-de>`. Commit theo dạng `type(scope): nội dung`, ví dụ `feat(etl): normalize interaction timestamps`.
3. Mở PR vào `main`. Mô tả thay đổi, issue liên quan, cách chạy thử, tác động tới dữ liệu/API và ảnh demo nếu có UI. Người còn lại review, rồi squash merge. Dùng PR draft để trao đổi nếu chưa đủ điều kiện merge.
4. Một việc hoàn tất khi có thể chạy theo hướng dẫn từ checkout sạch, các kiểm tra liên quan đạt, schema và docs được cập nhật, và người còn lại có thể dùng đầu ra. Đổi hợp đồng cần một PR được cả hai duyệt, ghi cách chuyển đổi dữ liệu cũ nếu đã có.

## 6. Mốc làm việc

| Mốc | Việc của Thành | Việc của Pháp | Kiểm tra chung |
| --- | --- | --- | --- |
| **M0 · Hợp đồng và fixture** | Chốt nguồn catalog khả thi và dựng mẫu sản phẩm nhỏ. | Dựng mẫu event theo schema, migration và API nhận event. | Cùng chạy với một bộ fixture. Event lặp ID chỉ sinh một bản ghi. |
| **M1 · Luồng dọc** | Chạy Bronze → Silver → Gold và xuất popularity theo batch. | Làm trang danh mục, trang chi tiết và giỏ hàng. Ghi log, xuất log sang Bronze và đọc popularity từ API. | Gửi một event từ UI, xuất Bronze, chạy batch và hiển thị snapshot qua API. |
| **M2 · Ba nhóm gợi ý** | Huấn luyện TF-IDF và ALS, xuất top-K cá nhân hóa và thay thế, thử MMR. | Tạo luật FP-Growth, kiểm tra tương thích khi có thông số và hoàn thiện UI của ba nhóm. | Kiểm tra user mới, trang chi tiết, giỏ rỗng, giỏ có dữ liệu và snapshot lỗi. |
| **M3 · Thực nghiệm và nộp bài** | So sánh chất lượng, đo thời gian ETL và huấn luyện, tổng hợp giới hạn. | Đo p50/p95 API và hoàn thiện demo tái lập. | Chạy lại từ checkout sạch và chốt báo cáo, slide cùng dữ liệu thử. |

Hai việc mở đầu là **M0 catalog fixture + chọn nguồn** do Thành phụ trách và **M0 event fixture + schema PostgreSQL** do Pháp phụ trách.
