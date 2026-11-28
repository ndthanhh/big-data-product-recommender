# Review độc lập toàn nhánh phần Thành

Range đã review: `b59dc55..63df300`. Reviewer đọc code độc lập, tái hiện với file tạm và in-memory clients; không sửa checkout hay DB và không chạy lại suite thay implementer.

## Các phát hiện đã sửa và thêm regression test

1. MinIO có thể upload file nguồn bị thay sau validation hoặc ghi đè batch đã công bố. Sửa: bản sao staging riêng được kiểm hash rồi upload; manifest remote khớp là no-op, không ghi lại object.
2. Gold hash byte rồi mở lại file Parquet có thể đọc nội dung mới. Sửa: PyArrow đọc chính buffer đã qua checksum.
3. Event bị từ chối vĩnh viễn chặn hàng đợi UI. Sửa: giữ status/code, tách failed_events, tiếp tục các event độc lập; lỗi mạng/5xx vẫn retry UUID cũ.
4. Xóa giỏ giữ cart_id làm hai giỏ trở thành một basket khi train. Sửa: đổi cart_id, giữ session và payload đã xếp hàng.
5. UI ghép gợi ý run A với metadata run B khi ID vẫn tồn tại. Sửa: metadata GET có expected-run header; mismatch làm client tải lại cả nhóm một lần trước render.
6. Product ID hợp lệ chứa `/` không qua route detail. Sửa: path converter cùng validation ProductId.
7. Runner thông báo run cũ dù serving pointer đang ở run mới. Sửa: kiểm tra active pointer sau import, dừng kèm lệnh activate tường minh.

Tám regression tests mới đã được quan sát fail trước sửa. Nhóm liên quan 41 tests pass sau sửa; toàn bộ suite sau sửa: 450 tests pass (140.14s), Ruff và format 58 file pass. MinIO thật cũng kiểm tra staged publication/no-op/tải 1.000 dòng thành công. Không dùng review này làm bằng chứng chất lượng mô hình thật.

## Minor để lại

Validator kiểm tra thứ tự fallback bằng cách quét lại recommendation rows theo từng user; tối ưu nhóm theo context thành một pass còn để sau. Có thể chậm khi snapshot đạt 1.000 user × 200 candidates; tính đúng đắn không thay đổi.

## Phạm vi cần bằng chứng bên ngoài

- Kết nối MinIO/tài khoản trên máy Pháp chưa có quyền truy cập để nghiệm thu.
- Chất lượng model, vòng retrain và benchmark đầy đủ cần Silver/Gold thực của Pháp.
- Kết quả test/benchmark/browser do implementer chạy; reviewer không xác nhận lại bằng một lượt test độc lập. Regression và full-suite sau review là lượt xác minh của implementer.
