# Báo cáo triển khai phần Nguyễn Đức Thành

Ngày kiểm tra: 25/11/2026. Nhánh `codex/thanh-remaining-work`; [nhật ký từng task](superpowers/plans/2026-11-25-thanh-progress.md) ghi commit và phạm vi nghiệm thu.

## Đầu vào và luồng dữ liệu

Catalog seed có 1.000 sản phẩm Best Buy chuẩn hóa và 1.000 hồ sơ người dùng. Bộ sinh tạo hành vi có sở thích, phân bố phổ biến, chuỗi view/click/add_to_cart và giỏ nhiều item. Bronze đang có khoảng 120 nghìn sự kiện; replay streaming có thể trùng UUID. Dữ liệu mô phỏng cần được ETL dedup trước huấn luyện và đánh giá.

Kafka tách bộ sinh khỏi consumer. Consumer lưu batch local và upload data trước manifest; dừng có kiểm soát flush batch chưa đầy. Smoke Kafka→MinIO nhận đủ 8/8 event, hai batch; script start/stop đã chạy thật. Công cụ chia sẻ kiểm tra byte, SHA-256 và số dòng, tải vào staging rồi đổi thư mục đích. Kiểm tra MinIO local với catalog/users đủ 1.000 dòng và batch events thành công; máy Pháp còn cần nghiệm thu mạng/quyền.

## Lưu trữ và phục vụ

PostgreSQL có bảy bảng nghiệp vụ, catalog khóa kép theo run và log demo có UUID/received_at/catalog_run_id. SQL migration có phiên bản và đóng gói cùng package. Importer kiểm tra Parquet, checksum, schema, keys, ranks, cutoff và quan hệ catalog; cùng fingerprint được retry, lỗi giữ snapshot cũ. Run ready cũ chỉ được kích hoạt lại qua lệnh rollback tường minh.

FastAPI phục vụ health, catalog list/detail, events và ba ngữ cảnh gợi ý. Event cùng UUID/payload trả 200, mới trả 201; conflict trả 409. Advisory locks ngăn hai UUID khác nhau chiếm cùng session/cart cho hai chủ sở hữu. Request đọc giữ một run bằng REPEATABLE READ kể cả khi importer kích hoạt run khác.

FP-Growth dùng PySpark 3.5.7. Giỏ được dedup và chỉ đưa giỏ từ hai item vào fit. Test tay với ba giỏ hợp lệ cho luật A→B có support=2/3, confidence=2/3, lift=1.0; giỏ một item không tăng mẫu số. API mua kèm khớp antecedent theo tập con, chọn luật tốt nhất cho mỗi consequent và không gợi ý lại món trong giỏ. Compatibility chỉ có ý nghĩa khi cấu hình thuộc tính rõ ràng.

## Phản hồi theo phiên và giao diện

Phiên dùng tối đa 20 event trong 30 phút, trọng số 1/2/4 và half-life 10 phút. Similarity lấy từ substitute TF-IDF của snapshot. Nguồn batch và phiên chuẩn hóa riêng, kết hợp 0.7/0.3; các row popularity bù không được chuẩn hóa chung với ALS/TF-IDF. Mọi item trong cửa sổ phiên và seen snapshot bị loại. Click vừa commit đã đổi B→C trong fixture, giữ nguyên run; session khác giữ nguyên danh sách.

Streamlit giữ user/session/cart và hàng đợi payload có UUID. Rerun/toggle/tab không tạo event; retry giữ UUID cũ. Kiểm tra AppTest và browser thật bao phủ danh mục→chi tiết→giỏ; [ảnh giỏ](demo/cart-fixture.jpg). Exporter đọc đầy đủ event demo trong snapshot nhất quán, giữ tám trường nghiệp vụ, để ETL xử lý lại an toàn cả khi transaction cũ commit muộn.

## Số đo phục vụ đã chạy

Uvicorn HTTP local, PostgreSQL Docker, Windows/Python 3.11.9, 16 logical CPU. Database benchmark tách khỏi test và demo. Mỗi route warm-up 3 request, đo 100 request, concurrency 4. Fixture chỉ có **5 sản phẩm**, không phải catalog mô hình thật.

| Route | p50 (ms) | p95 (ms) | Lỗi / 100 |
| --- | ---: | ---: | ---: |
| Personalized | 37.52 | 59.83 | 0 |
| Substitutes | 42.77 | 62.17 | 0 |
| Complements | 45.04 | 72.54 | 0 |
| Events | 50.29 | 85.93 | 0 |

[JSON kết quả và 400 mẫu thô](results/api-fixture-benchmark.json) có run_id, SHA-256 manifest và cấu hình máy. Percentile nội suy tuyến tính trên độ trễ HTTP; không gồm thời gian train. Một lượt local fixture chỉ xác nhận đường phục vụ hoạt động, chưa chứng minh tải lớn hoặc SLA.

## Giới hạn và bàn giao tiếp

Pháp chưa bàn giao ETL, output popularity/TF-IDF/ALS/MMR, split và bảng metric thật trong checkout này. Vì vậy chưa có kết luận Recall/NDCG/diversity hoặc vòng export→retrain→Gold thật. Việc chọn ngưỡng FP-Growth, benchmark catalog 1.000 sản phẩm và chạy từ tài khoản teammate cần nghiệm thu tiếp. [Hướng dẫn demo](DEMO.md) có các lệnh tái lập, importer/rollback và checklist bàn giao.

Kiểm tra cuối sau [review độc lập](REVIEW.md): 450 tests pass, Ruff/format pass; đã sửa bảy vấn đề toàn vẹn/lifecycle và thêm tám regression tests. Tối ưu quét fallback theo user còn để sau, có thể ảnh hưởng thời gian validate snapshot lớn.
