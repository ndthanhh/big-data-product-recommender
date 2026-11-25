# Nhật ký thực thi kế hoạch phần việc Thành

Kế hoạch [A00–A11](2026-11-25-thanh-remaining-work.md). Bắt đầu từ commit b59dc55.

## Quyết định triển khai

- Làm trên nhánh codex/thanh-remaining-work trong checkout hiện có để tiếp tục các thay đổi streaming người dùng đang làm dở. Push nhánh sau mỗi task theo yêu cầu.
- Fixture Gold dùng schemas hiện tại, đọc Parquet bằng PyArrow. A10 nghiệm thu snapshot thật phụ thuộc bàn giao của Pháp.
- Snapshot validator, complements và session dùng cùng kiểu Pydantic, API đọc connection với run cố định. A08 xuất lại toàn bộ log demo theo đặc tả để tránh bỏ sót transaction commit muộn.

## Tiến độ

- A00 đang thực hiện. Baseline gần nhất 376 test pass, 8 lỗi lint đã xác định.

- A00 hoàn thành. Producer/consumer hỗ trợ cooperative stop, worker truyền lỗi, script xác minh PID và xử lý PID cũ đã hết. Kiểm tra Kafka→MinIO 8/8 sự kiện, 2 batch; start/stop thực tế thành công. Toàn bộ suite 383 tests.
- Ruling A00. Image minio/mc hiện không tải được, nghiệm thu dùng docker compose up -d postgres kafka minio. Client Python tạo bucket khi ghi, hướng dẫn đã cập nhật.

- A00 đã push: d470ec877c65b835a4faf58c8463b4572170b63f (SHA remote khớp).
- A01 code hoàn thành: 387 tests pass, Ruff pass. MinIO roundtrip catalog/users 1.000 dòng và 2 batch/8 events đúng hash. Chờ endpoint LAN và tài khoản thực trên máy Pháp.
- Ruling A01: sync trả danh sách prefix thay vì None để hỗ trợ tải chính xác batch; prefix chứa fingerprint manifest, manifest upload cuối. minio-init chuyển profile tùy chọn do image mc không tải được; Python tạo bucket. Chi phí: máy thứ hai vẫn cần xác minh mạng/quyền riêng.

- A01 push be950dcfb0483e91f29066aaad47e9463c3f6737, SHA remote khớp.
- A02 hoàn thành: 9 tests DB thật, migration lặp và packaged SQL resource đúng; Ruff pass.
- Ruling A02: POSTGRES_PORT cấu hình mapping Docker, local .env dùng 5433 do postgres Windows chiếm 5432; defaults vẫn 5432. Test chỉ truncate database *_test đã chọn tường minh. Chi phí: teammate cần chọn đúng cổng local.

- A02 push 6974f41e0935c8a66108a003a85bc33d41d7d951, SHA remote khớp.
- A03 hoàn thành: 13 tests importer (22 gồm DB), checksums/Parquet/keys/as-of/empty rules; rollback giữ catalog, cạnh tranh cùng/khác run, retry ready no-op.
- Ruling A03: importer nạp bốn bảng serving và kiểm tra byte mọi artifact liệt kê; validation nội dung model/vocabulary/ALS mappings thuộc publisher Gold của Pháp. Fixture không đại diện kết quả train. Chi phí: nghiệm thu artifact model thật cần A10. PyArrow thêm app dependency.

- A03 push 3c8c1837f315710a2646cdc54e5d21dec4b35972, SHA remote khớp.
- A04: 5 tests pass (Spark thật 35.73s). Support/confidence 2/3, lift 1.0; giỏ một item loại khỏi mẫu số; lọc compatibility và dedup consequent. Runner lưu model/rules/config, cần nối Gold publisher của Pháp. Tham khảo https://spark.apache.org/docs/3.5.7/ml-frequent-pattern-mining.html (Spark chỉ phát luật một consequent).

- A04 push 0913db5f2499b51d13c537080837f14b231f8718, SHA remote khớp.
- A05 hoàn thành 9 tests DB/API và Uvicorn HTTP thật: health/catalog 200; UUID retry/conflict; ownership locks concurrent; commit failure 503 không lưu event. Ruff pass.

- A05 push 85299918b9123845dfc245394e143993e167a6f1, SHA remote khớp.
- A06: 13 tests API pass. Personalized lọc seen/bù popularity, substitute cùng category, complements subset/rỗng/ID lỗi; request giữ run cũ khi con trỏ đổi, thứ tự MMR giữ nguyên. Ruff pass.

- A06 push 9ebe48609a3065c543c1b8f5416b8cb0229b70a7, SHA remote khớp.
- A07: 9 tests session/API pass; weights 1/2/4, half-life 10m, 30m/future/20event, normalizations riêng, before/after click cùng run đổi b→c, session khác không đổi và user khác 409. Ruff pass.

- A07 push 28c76b9466f69a5fb947494c85a655b4eef8d705, SHA remote khớp.
- A08: 8 tests exporter/sync pass; full consistent export tám trường, late commit xuất lần sau, UUID nguyên, previous batch và DB giữ nguyên khi lỗi ghi. Hidden staging được publisher bỏ qua (test RED→GREEN). Ruff pass.

- A08 push 69241044edc5ee1d3746fd2660dfd87b2b2a8ced, SHA remote khớp.
- A09: 4 tests state/client/AppTest pass; rerun không nhân events, đổi user/giỏ/UUID ổn định, timeout giữ pending. Browser thật với fixture_demo: Product a → substitute c → thêm giỏ → complement d; ảnh docs/demo/cart-fixture.jpg. API/UI chạy local cổng 8000/8501.
- Ruling A09: ảnh catalogue dùng biểu tượng, ảnh chi tiết chỉ hiển thị sau kiểm tra HTTP/định dạng và có fallback. Nâng minimum Streamlit 1.54 cho widget width, thêm HTTPX vào app dependency. Chi phí: tải metadata từng gợi ý làm UI chậm hơn, benchmark API tách riêng.

- A09 push 78aad25ebba738e9b54828fadf6d329b9618899e, SHA remote khớp.
- A10 phần độc lập hoàn thành: 2 E2E tests pass; runner và fixture CLI; HTTP benchmark Uvicorn 400 mẫu, concurrency4, warmup3, 0 lỗi, DB recommender_benchmark riêng. P95 personalized59.83ms/substitutes62.17ms/complements72.54ms/events85.93ms, chỉ 5 sản phẩm fixture.
- Ruling A10: Silver/Gold model thật của Pháp chưa có; giữ các bước nghiệm thu model/vòng retrain ở trạng thái chờ. Không tạo ETL/ALS thay teammate hoặc coi fixture là model đã train. Chi phí: chưa kết luận chất lượng và hiệu năng với 1.000 sản phẩm/120k events.
