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
