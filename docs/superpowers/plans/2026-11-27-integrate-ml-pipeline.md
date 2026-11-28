# Tích hợp ML pipeline của Pháp với serving của Thành

> Thực thi trực tiếp theo superpowers:executing-plans; yêu cầu người dùng đã duyệt phương án ghép và sửa xung đột. Giữ checkout hiện có vì người dùng đã mở merge trên nhánh codex/integrate-ml-pipeline.

**Mục tiêu:** Hoàn tất merge 767c97a + 543b001, giữ đủ ETL/train/publish và validator/importer/API/UI, kiểm thử rồi push nhánh tích hợp để review trước main.

**Kiến trúc:** Giữ pipeline/snapshot.py cho validate_snapshot; chuyển publisher của Pháp sang pipeline/publish_snapshot.py. Một lõi fit_fpgrowth phục vụ API fit_rules bằng adapter. Giữ fixture Spark và DB cùng nhau.

**Đặc tả:** docs/DATA_CONTRACTS.md, docs/ARCHITECTURE.md, PR #1. Python 3.11, Java17, Spark3.5.7, PostgreSQL17, UTC, schema1. Không merge vào main hoặc sửa nhánh Pháp. Chỉ dùng DB *_test tách biệt.

## Công việc và kiểm tra

- [x] I01: Thêm test bàn giao pipeline → Gold validator/importer → API. Quan sát fail do merge chưa xử lý. Giải quyết năm file xung đột; cập nhật import của publisher và benchmark, giữ CLI FP-Growth. Check hết conflict markers và import/pytest collection.
- [x] I02: Chạy test hai bên, sửa các bất tương thích schema/manifest/thời gian theo hợp đồng. Chạy native Spark/Parquet/model save trên Linux bằng Python3.11/Java17/Spark3.5.7; ghi runtime thực. Test mới chứng minh publisher output được importer chấp nhận, ba nhóm API đọc cùng run, event và export được giữ.
- [x] I03: Nghiệm thu Gold từ dataset thực 1.000 sản phẩm nếu runtime cho phép; dùng DB tích hợp riêng, không đổi con trỏ demo hoặc xóa data cũ. Test retry/idempotency và schema thống kê.
- [x] I04: Review độc lập một lần cả phần tích hợp; sửa Important/Critical bằng regression test. Cập nhật tài liệu/lệnh/ownership theo code đã gộp; pytest, Ruff, link tài liệu. Commit merge và push codex/integrate-ml-pipeline, đối chiếu SHA remote.

## Điểm cần review

- Hai module snapshot phải cùng hoạt động; không làm mất validator hay exporter.
- FP-Growth phải dedup item, giữ mẫu số giỏ >=2 và schema dùng chung; old/new call sites không gãy.
- Timestamp Spark local-naive phải được hiểu UTC đúng khi validator/importer đọc Parquet.
- Model và serving schemas/rank/fallback phải khớp; rỗng có schema; cả artifact native được đọc lại.
- Tests dùng DB test riêng; main và nhánh Pháp không đổi; không ghi fixture vào dữ liệu demo.

## Nhật ký

- Bắt đầu: nhánh tích hợp đã có MERGE_HEAD 543b001; 5 file unmerged. Giữ toàn bộ thay đổi auto-merge. Ruling: dùng checkout đang merge theo yêu cầu thay vì tạo worktree mới; chi phí là checkout tạm không chạy được cho tới khi giải quyết hết markers.

- I01: hết 5 unmerged entries, pytest collection481 lúc đầu. 463 tests ngoài nhóm Spark pass trên Windows; test handoff fixture train→Gold→import→API/event/export pass Linux.
- I02: phát hiện layout lệch datasets/demo-events so với ETL. Hai test RED→GREEN, thêm routing theo table và bỏ staging ẩn. Versioned prefix giữ bất biến, không đổi cấu trúc object đã công bố.
- Runtime: Python3.11.9, Java17, Spark3.5.7; base container thiếu Git làm 3 test provenance fail. Đã bổ sung Git và trust riêng /workspace read-only. Pin BLAS1 để không oversubscribe CPU khi MMR.
- Review độc lập duy nhất: 4 Important, 0 Critical/Minor. Đã quan sát 6 test pure fail và native Gold model test fail trước sửa. Sửa metadata theo manifest được chấp nhận, ETL version1.2.0; ID có hai mốc train và evaluation config; guard retry khác input; model FP-Growth trong Gold và reload; download vào typed layout.
- Ruling: chạy trực tiếp trong checkout đang merge do người dùng tạo; không đổi nhánh Pháp/main. Chi phí: checkout chưa ổn định trong thời gian ghép, chỉ commit sau kiểm tra.
- Ruling: metadata mixed seeds/generator được giữ bằng danh sách, top-level chỉ có giá trị khi duy nhất; model params/provenance do manifest có file accepted cung cấp. Chi phí: consumers metadata cũ dạng scalar cần đọc danh sách mới; API serving không thay đổi.
- Ruling: publisher cùng run cần chạy tuần tự trong demo, chưa thêm khóa phân tán. Importer vẫn có khóa cạnh tranh. Chi phí: hai publisher cùng run đồng thời có thể ghi chồng artifact; cần tính năng riêng nếu nhóm muốn chạy kiểu đó.
- Ruling: review không chạy live hai máy hoặc xác nhận quality/throughput; nghiệm thu dữ liệu 1.000/100.000 do implementer chạy tách biệt. Chi phí: chưa xác nhận LAN/tài khoản máy Pháp và không coi smoke là benchmark chất lượng mới.

- Final I02/I03: Linux487pass/3Windows-skip/466.35s; Windows16 regression/PowerShell pass sau sửa; Ruff/format79 files pass. Gold thực ds-de5473c78974b177e18d136b, run merge_20261108T000000Z_alsmmr_42_62b0aa1d: 1.000 products, 100.000 input events, 282.536 recommendations (200.000 personalized +82.336 substitutes +200 popular), 746 rules, native FP model trong manifest. Import vào DB recommender_integration_test; 3 nhóm API, session, event201→200 và export1event đúng. JSON evidence trong docs/results/integration-*.json.
- Review fixes đều RED→GREEN: provenance/versioned source metadata; training start/grid identity và reuse guard; native FP model thiếu trong Gold; typed download được ETL đọc. Một fix pass, không re-review. Không có minor được hoãn.
- Các input đã stage/unstaged của merge được giữ; shared main và feat/ml-pipeline chưa thay đổi. Chốt merge commit sau tất cả kiểm tra, push nhánh tích hợp và xác nhận remote SHA.
