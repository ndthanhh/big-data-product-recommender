# Checklist hoàn thành dự án gợi ý sản phẩm

Cập nhật ngày 13/11/2026. Áp dụng cho dự án hai người theo đề cương Nhóm 1, gồm thu thập catalog, sinh hành vi, Data Lake, ETL PySpark, ba nhóm gợi ý, API, website demo và báo cáo thực nghiệm. Bổ sung luồng gợi ý theo phiên và chia sẻ dataset tự động từ các trao đổi sau đề cương.

[Trang chủ repo](../README.md) · [Quy ước đồng nhất](CONVENTIONS.md) · [Vận hành](README.md) · [Kiến trúc](ARCHITECTURE.md) · [Hợp đồng v1](DATA_CONTRACTS.md) · [Thực nghiệm](EXPERIMENT_PLAN.md) · [Kế hoạch & Phân công](../PLAN.md)

## Cách dùng và trạng thái hiện tại

Đánh dấu `[x]` khi đã có đầu ra kiểm tra được. Một việc đang làm vẫn để `[ ]` và có thể ghi issue hoặc PR bên cạnh. Các đường dẫn đầu ra trong checklist là vị trí dự kiến trong repo. Lệnh chạy cụ thể được điền vào tài liệu khi entry point tương ứng đã hoạt động.

- [x] Có cấu trúc `src/recommender/`, `tests/fixtures/` và `data/bronze/`, `data/silver/`, `data/gold/`.
- [x] Có cấu hình Python, mẫu biến môi trường và quy tắc bỏ qua dữ liệu sinh ra.
- [x] Có [README gốc](../README.md), [hướng dẫn vận hành](README.md), [kế hoạch & phân công](../PLAN.md) và checklist này.
- [x] T01 có [kiến trúc](ARCHITECTURE.md), [hợp đồng dữ liệu/PostgreSQL/API v1](DATA_CONTRACTS.md), [kế hoạch thực nghiệm](EXPERIMENT_PLAN.md) và chỉ mục tài liệu thống nhất.
- [ ] Chốt phân công cập nhật và lựa chọn kho chia sẻ dataset.
- [ ] Hoàn thành một lượt chạy từ dữ liệu đầu vào đến gợi ý trên website.
- [ ] Hoàn thành mô hình, thực nghiệm, demo và bộ tài liệu nộp bài.

### Vai trò dùng trong kế hoạch

| Vai trò | Người phụ trách trong tài liệu | Phạm vi |
| --- | --- | --- |
| A | Nguyễn Đức Thành | Thu thập catalog, bộ sinh, xuất log, công bố dataset, PostgreSQL, importer, API và UI. |
| B | Phan Văn Pháp | ETL PySpark, tải dataset, chia tập, toàn bộ mô hình kể cả FP-Growth, đánh giá và xuất snapshot phục vụ. |
| A + B | Cả hai | Chốt hợp đồng, kiểm tra dữ liệu, tích hợp snapshot, gợi ý theo phiên, demo và báo cáo. |

Ánh xạ A/B thống nhất với [PLAN.md](../PLAN.md); hai người review hoặc điều chỉnh ở P0-04. T01 hoàn thiện đặc tả, không xác nhận nhóm đã duyệt phân công hay các chức năng đã chạy. Các mức 10 nghìn, 100 nghìn, 1 triệu sự kiện và giao thức đo nằm trong [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md).

### Nguồn đặc tả và nghiệm thu T01

Schema, nullable, enum, trọng số `view=1`, `click=2`, `add_to_cart=4`, bảy bảng PostgreSQL và API lấy từ [DATA_CONTRACTS.md](DATA_CONTRACTS.md). Luồng snapshot/session theo [ARCHITECTURE.md](ARCHITECTURE.md); split, relevance, metric và benchmark theo [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md). Không coi checkbox về tài liệu là kết quả kiểm thử các mục P4–P13.

- [x] Có đủ README gốc và sáu file trong `docs/`, phân biệt trạng thái scaffold với đặc tả cho các task sau.
- [x] Kiểm tra file/link relative, cấu trúc Markdown, UTF-8/LF, JSON ví dụ, trọng số và ví dụ metric tính tay của bộ tài liệu T01.
- [ ] Review nhóm đối với phân công, nguồn catalog, kho chia sẻ và ngân sách ở P0; chỉ đánh dấu sau khi có xác nhận thực tế.

## P0. Chốt phạm vi, hợp đồng và cách phối hợp

Phụ trách A + B. Làm trước các phần triển khai.

- [ ] **P0-01** Chốt danh mục sản phẩm và nguồn catalog được phép lấy, gồm tên nguồn, cách truy cập và trường thông tin có thể thu thập.
- [ ] **P0-02** Chốt ba kết quả bắt buộc trên demo gồm Dành cho bạn, Sản phẩm thay thế và Thường thêm cùng.
- [ ] **P0-03** Chốt phần mở rộng theo phiên, gồm thời điểm cập nhật sau click hoặc thêm giỏ và cách so sánh với gợi ý batch.
- [ ] **P0-04** Review phân công theo tên A/B, chủ sở hữu module và người review đã được viết trong [PLAN.md](../PLAN.md).
- [ ] **P0-05** Review và duyệt schema v1 trong [DATA_CONTRACTS.md](DATA_CONTRACTS.md): sản phẩm/sự kiện, ID chuỗi, UTC, source, trọng số và cart bắt buộc.
- [ ] **P0-06** Chốt người thực hiện Bronze → Silver và Silver → Gold, định nghĩa rõ bộ dữ liệu bàn giao cho người train.
- [ ] **P0-07** Chốt kho chia sẻ dữ liệu và cách xác thực. Phương án đang đề xuất là tự động công bố và tải dataset qua GitHub Releases API. Ghi quyết định cuối trong tài liệu.
- [ ] **P0-08** Review hợp đồng personalized có `session_id` tùy chọn, fallback user mới/phiên rỗng và tham số phiên đã viết trong [DATA_CONTRACTS.md](DATA_CONTRACTS.md).
- [ ] **P0-09** Chốt ngân sách tài nguyên, hạn nộp, các mốc nghiệm thu và tạo issue tương ứng từ checklist.

Đầu ra là [PLAN.md](../PLAN.md) và [DATA_CONTRACTS.md](DATA_CONTRACTS.md) đã được cả hai duyệt, lựa chọn kho chia sẻ và danh sách issue có chủ sở hữu. Hai người phải mô tả được đầu vào và đầu ra của phần mình bằng cùng một bộ fixture.

## P1. Thiết lập môi trường chạy trên hai máy

Phụ trách A + B. Phụ thuộc P0.

- [ ] **P1-01** Cài Python 3.11, JDK 17, Git và tạo môi trường Python riêng trên mỗi máy.
- [ ] **P1-02** Cài dependency từ `pyproject.toml`, xác nhận PySpark 3.5.7 khởi tạo được Spark local.
- [ ] **P1-03** Chạy thử đọc JSONL và ghi, đọc lại Parquet trên cả hai máy, xử lý các lỗi Java và đường dẫn nếu có.
- [ ] **P1-04** Thiết lập PostgreSQL trên máy chạy API, tạo database và thông tin truy cập local.
- [ ] **P1-05** Tạo `.env` từ mẫu, thống nhất cách đọc cấu hình, thư mục gốc và seed. Bổ sung biến cho kho dataset đã chọn.
- [ ] **P1-06** Bổ sung dependency thực sự cần khi triển khai, chốt phiên bản đã chạy thử và ghi hướng dẫn cài đặt từ checkout sạch vào [hướng dẫn vận hành](README.md).
- [ ] **P1-07** Cấp quyền repo cho teammate và kiểm tra teammate clone, tạo nhánh, mở PR và truy cập dataset riêng tư bằng tài khoản của mình.

Đầu ra là hai máy chạy được cùng một fixture qua Spark và người làm API kết nối được PostgreSQL.

## P2. Thu thập và chuẩn hóa catalog đầu vào

Phụ trách A, B review schema. Phụ thuộc P0-01 và P0-05.

- [ ] **P2-01** Viết module lấy catalog trong `src/recommender/ingestion/`, có timeout, phân trang nếu nguồn hỗ trợ và retry có giới hạn.
- [ ] **P2-02** Chuẩn hóa `product_id` ổn định theo nguồn, `category_id`, tên, mô tả, ảnh, URL và các thông số có thể so sánh.
- [ ] **P2-03** Lưu JSONL gốc theo đợt vào `data/bronze/products/`, kèm nguồn, thời điểm lấy và thông tin đợt thu thập.
- [ ] **P2-04** Giữ cách đối chiếu giữa dữ liệu nguồn và sản phẩm chuẩn hóa để tra lỗi ID, trường thiếu và sản phẩm ngừng xuất hiện.
- [ ] **P2-05** Tạo catalog fixture nhỏ, gồm nhiều sản phẩm cùng danh mục để thử thay thế và các sản phẩm bổ sung để thử mua kèm.
- [ ] **P2-06** Kiểm tra trường thiếu, ID trùng, ảnh không tải được và danh mục thiếu thông số tương thích. Ghi số lượng theo từng trường hợp.

Đầu ra là catalog Bronze và fixture trong `tests/fixtures/`. Chạy lại cùng đợt dữ liệu phải giữ được ID sản phẩm đã cấp.

## P3. Sinh dữ liệu hành vi có thể dùng để học

Phụ trách A, B review ý nghĩa dữ liệu. Phụ thuộc catalog fixture P2 và hợp đồng P0.

- [ ] **P3-01** Viết bộ sinh nhận catalog, số user, số phiên, số sự kiện, khoảng thời gian và seed qua cấu hình hoặc CLI.
- [ ] **P3-02** Sinh đúng `event_id`, `user_id`, `session_id`, `product_id`, `event_type`, `event_at`, `source=synthetic` và `cart_id` khi thêm giỏ.
- [ ] **P3-03** Mô phỏng nhóm sở thích, độ phổ biến lệch giữa sản phẩm, hành vi khám phá và nhiễu để tạo tín hiệu học có thể giải thích.
- [ ] **P3-04** Sinh chuỗi hành vi có thứ tự thời gian trong phiên và các giỏ nhiều sản phẩm để có dữ liệu cho FP-Growth.
- [ ] **P3-05** Bổ sung ca user mới, item mới và phiên thay đổi danh mục quan tâm để kiểm tra fallback và gợi ý theo phiên.
- [ ] **P3-06** Ghi dữ liệu lớn theo từng phần vào `data/bronze/events/`, kiểm soát bộ nhớ và bảo đảm retry hoặc chạy lại giữ được phiên bản dữ liệu nhất quán.
- [ ] **P3-07** Ghi manifest gồm phiên bản bộ sinh, commit code, seed, phiên bản catalog, tham số, số user, item, phiên, giỏ, sự kiện và khoảng thời gian.
- [ ] **P3-08** Sinh trước một bộ nhỏ để teammate bắt đầu code, sau đó sinh các mức 10 nghìn, 100 nghìn và 1 triệu sự kiện theo khả năng máy.
- [ ] **P3-09** Kiểm tra toàn vẹn ID sản phẩm, thứ tự thời gian, tính duy nhất của event và giỏ, phân bố hành vi và khả năng tái lập cùng seed.

Đầu ra là Bronze có nguồn gốc rõ ràng và manifest. Dữ liệu phải có quy luật đủ để thử mô hình, đồng thời ghi rõ các giả định mô phỏng trong báo cáo.

## P4. ETL PySpark và Data Lake

Phụ trách B, A hỗ trợ dữ liệu nguồn. Phụ thuộc fixture P2 và P3.

- [ ] **P4-01** Khai báo schema khi đọc catalog và events, xử lý timestamp UTC, chuỗi rỗng và bản ghi không đọc được.
- [ ] **P4-02** Khử trùng sản phẩm theo quy tắc đã chốt và sự kiện theo `event_id`. Phát hiện cùng ID nhưng nội dung khác nhau.
- [ ] **P4-03** Kiểm tra event tham chiếu đến catalog, loại hành vi hợp lệ và cart bắt buộc khi thêm giỏ. Ghi số dòng nhận, hợp lệ và bị loại theo lý do.
- [ ] **P4-04** Ghi catalog và events sạch thành Parquet trong `data/silver/products/` và `data/silver/events/`.
- [ ] **P4-05** Gắn Silver với `dataset_version`, manifest và checksum. Công bố một phiên bản sau khi toàn bộ file cần thiết đã ghi và kiểm tra xong.
- [ ] **P4-06** Viết bước đọc Silver để tổng hợp tương tác có trọng số và tạo tập giỏ riêng cho từng cửa sổ train được chọn.
- [ ] **P4-07** Chốt bố trí Gold theo `run_id`, lưu các bảng phục vụ, manifest, cấu hình, ánh xạ ID, model và kết quả đánh giá có liên kết với dataset.
- [ ] **P4-08** Kiểm tra chạy lại, đầu vào rỗng, batch dở dang và thư mục đã có kết quả. Giữ bản thành công trước đó có thể dùng tiếp.

Đầu ra là entry point ETL có hướng dẫn chạy và Silver đọc được từ máy teammate. Thư mục, schema, lịch sử metadata và manifest theo [DATA_CONTRACTS.md](DATA_CONTRACTS.md); luồng dữ liệu theo [ARCHITECTURE.md](ARCHITECTURE.md).

## P5. Tự động chia sẻ dataset giữa hai máy

Phụ trách A viết bước công bố, B viết bước tải và tích hợp vào train. Phụ thuộc P0-07 và fixture Silver P4.

- [ ] **P5-01** Chốt gói dữ liệu có catalog, events Silver và manifest. Tách định danh `dataset_version` của đầu vào với `run_id` của từng lần train.
- [ ] **P5-02** Viết bước đóng gói và upload tự động sau khi sinh, ETL và kiểm tra dữ liệu thành công.
- [ ] **P5-03** Nếu chọn GitHub Releases, dùng bản nháp khi upload, xử lý kích thước từng file theo giới hạn dịch vụ và chỉ publish khi mọi thành phần đã đầy đủ.
- [ ] **P5-04** Viết bước tải theo phiên bản cụ thể bằng tài khoản có quyền đọc, tải vào vị trí tạm và kiểm tra manifest, checksum trước khi kích hoạt bản local.
- [ ] **P5-05** Cache dataset đã tải để các lần thử model dùng lại. Cho phép retry download sau lỗi mạng và từ chối dùng file thiếu hoặc sai checksum.
- [ ] **P5-06** Tích hợp chọn phiên bản và tải vào entry point train. Mỗi lượt train cố định phiên bản đầu vào từ đầu đến cuối.
- [ ] **P5-07** Lưu thông tin xác thực qua môi trường local, phân quyền người công bố và người tải theo vai trò.
- [ ] **P5-08** Chạy nghiệm thu trên máy teammate bằng tài khoản và đường dẫn riêng, ghi các lệnh đã chạy được vào [hướng dẫn vận hành](README.md).

Đầu ra là một luồng tự động từ công bố dataset đến máy khác tải và train. Khi bạn phát hành bộ mới, teammate vẫn có thể chạy lại một phiên bản cũ.

## P6. Chia dữ liệu và định nghĩa đánh giá trước khi train

Phụ trách B, A review. Phụ thuộc Silver P4.

- [ ] **P6-01** Triển khai hai cutoff UTC và khoảng nửa mở theo [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md), lưu cấu hình; đổi lịch dataset phải chốt trước train.
- [ ] **P6-02** Fit popularity, ALS, hồ sơ nội dung và luật giỏ bằng dữ liệu có trước mốc dự đoán. Vectorizer và metadata dùng để dự đoán phải sẵn có ở thời điểm đó.
- [ ] **P6-03** Chốt relevance từ click và thêm giỏ, cách khử trùng nhãn, tập ứng viên và quy tắc loại item đã tương tác.
- [ ] **P6-04** Ghi rõ user đủ điều kiện đánh giá, user không còn nhãn sau lọc và các user hoặc item mới. Lưu số lượng của từng nhóm.
- [ ] **P6-05** Chia giỏ thành đơn vị nguyên vẹn, purge toàn bộ event gắn giỏ đi qua cutoff theo [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md); ghi số giỏ/event bị loại.
- [ ] **P6-06** Chuẩn bị fixture có kết quả Recall@10 và NDCG@10 tính tay để đối chiếu cách tính metric.

Đầu ra là split và metric thực thi theo [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md), dùng chung cho mọi mô hình. Tổng hợp trọng số và tạo giỏ học sau chia tập; test fixture đối chiếu ví dụ tính tay trong tài liệu.

## P7. Baseline và mô hình gợi ý

Phụ trách B. Phụ thuộc P6. Làm popularity trước để tích hợp website sớm.

### Popularity

- [ ] **P7-01** Tính độ phổ biến từ tập train theo trọng số hành vi và tạo danh sách phổ biến có thứ tự ổn định khi điểm bằng nhau.
- [ ] **P7-02** Lọc sản phẩm không còn hợp lệ và item user đã tương tác, xuất ứng viên đủ lớn để lấy top 10 sau lọc.
- [ ] **P7-03** Lưu baseline vào Gold và dùng nó làm đường chuẩn để so sánh, đồng thời làm fallback trong API.

### TF-IDF và gợi ý thay thế

- [ ] **P7-04** Tạo nội dung từ tên, danh mục và mô tả, xử lý trường rỗng, lưu vectorizer và cấu hình tiền xử lý.
- [ ] **P7-05** Tính sản phẩm tương tự bằng cosine, giới hạn cùng danh mục cho gợi ý thay thế và loại sản phẩm gốc.
- [ ] **P7-06** Tạo hồ sơ nội dung của user từ lịch sử có trọng số để có mô hình cá nhân hóa TF-IDF so sánh với ALS.
- [ ] **P7-07** Xuất danh sách tương tự cho các sản phẩm để API dùng cho trang chi tiết và lấy ứng viên theo phiên.

### Implicit ALS

- [ ] **P7-08** Tạo ánh xạ `user_id` và `product_id` sang số nguyên trong giới hạn ALS, lưu ánh xạ kèm model và `run_id`.
- [ ] **P7-09** Train với `implicitPrefs=True`, chốt không gian thử rank, số vòng lặp, regularization, alpha và seed theo khả năng máy.
- [ ] **P7-10** Chọn cấu hình trên validation, xử lý dự đoán thiếu cho user hoặc item mới bằng chính sách fallback đã chốt.
- [ ] **P7-11** Lưu model, tải lại để tính gợi ý và đối chiếu kết quả với model trong lần train.
- [ ] **P7-12** Sinh ứng viên, lọc item đã tương tác hoặc không có trong catalog phục vụ, loại điểm không hữu hạn rồi xuất top K bằng ID gốc.

### FP-Growth và mua kèm

- [ ] **P7-13** Tạo tập sản phẩm duy nhất cho từng giỏ trong train, thống kê số giỏ có từ hai sản phẩm và xử lý giỏ rỗng.
- [ ] **P7-14** Train FP-Growth, chọn minSupport và minConfidence theo dữ liệu thử, xuất antecedent, consequent, support, confidence và lift.
- [ ] **P7-15** Chuẩn hóa và khử trùng các ID trong antecedent, tách từng consequent nếu cần để phù hợp bảng phục vụ.
- [ ] **P7-16** Chọn luật khi antecedent nằm trong giỏ hiện tại, khử trùng kết quả, loại item đã có trong giỏ và xử lý trường hợp thiếu luật.
- [ ] **P7-17** Viết quy tắc tương thích cho những danh mục có đủ thông số, ghi rõ cách xử lý thông số thiếu và fixture cặp hợp lệ, không hợp lệ.

### MMR

- [ ] **P7-18** Chuẩn hóa điểm phù hợp trước khi kết hợp với cosine similarity, chốt cách xử lý ứng viên thiếu vector.
- [ ] **P7-19** Áp dụng MMR trên tập ứng viên lớn hơn K, chọn lambda trên validation và giữ thứ tự ổn định khi hòa điểm.
- [ ] **P7-20** Xuất cùng một lượt gợi ý có và chưa áp dụng MMR để đánh giá tác động tới độ phù hợp, đa dạng và độ phủ.

Đầu ra gồm model có thể tải lại, cấu hình và ánh xạ ID, danh sách gợi ý theo user, danh sách thay thế, luật mua kèm và baseline. Mỗi output ghi dataset và mốc dữ liệu đã dùng.

## P8. PostgreSQL và hợp đồng bàn giao snapshot

Phụ trách A, B review dữ liệu đầu ra. Bắt đầu với schema và fixture, tích hợp với popularity ngay khi P7-03 xong.

- [ ] **P8-01** Viết migration SQL cho `products`, `events`, `batch_runs`, `serving_state`, `recommendation_items`, `seen_products` và `association_rules`.
- [ ] **P8-02** Triển khai khóa/FK/nullable/CHECK theo [DATA_CONTRACTS.md](DATA_CONTRACTS.md), gồm products theo run, event tham chiếu catalog_run_id, rank/score và singleton.
- [ ] **P8-03** Giữ event bất biến theo `event_id`, thêm thời điểm server nhận dữ liệu để hỗ trợ xuất log ổn định và đối chiếu các sự kiện đến trễ.
- [ ] **P8-04** Tạo index phục vụ truy vấn các sự kiện gần nhất theo user và session, tra catalog, top K theo context và lịch sử item đã tương tác.
- [ ] **P8-05** Viết importer từ Gold, kiểm tra schema, ID, rank, bản ghi trùng, điểm không hữu hạn và sự đầy đủ của các bảng cùng `run_id`.
- [ ] **P8-06** Nạp catalog/kết quả và đổi active run trong một transaction, khóa singleton và kiểm tra cạnh tranh. Request đọc cùng run bằng REPEATABLE READ theo [ARCHITECTURE.md](ARCHITECTURE.md).
- [ ] **P8-07** Kiểm tra batch lỗi giữa chừng, import lại cùng batch, hai người phát hành đồng thời và khả năng quay về batch thành công trước đó.
- [ ] **P8-08** Viết exporter v1 quét lại đầy đủ events demo đã commit, giữ UUID, ghi mốc received_at để chẩn đoán; không bỏ sót commit muộn. File/manifest hoàn chỉnh trước công bố theo [ARCHITECTURE.md](ARCHITECTURE.md).
- [ ] **P8-09** Chốt thời gian giữ log và snapshot trong PostgreSQL theo nhu cầu replay, lọc lịch sử và dung lượng đo được. Kiểm tra bản lưu Data Lake trước khi dọn dữ liệu cũ.

Đầu ra là schema tạo lại được từ migration, importer có thể retry và fixture snapshot nhỏ. Bộ dữ liệu mô phỏng lớn đi vào Bronze để thực nghiệm pipeline.

## P9. API phục vụ dữ liệu và gợi ý

Phụ trách A. Có thể viết bằng fixture song song P7 và P8.

- [ ] **P9-01** Tạo FastAPI app, cấu hình kết nối PostgreSQL, timeout và xử lý lỗi có thể chẩn đoán qua log.
- [ ] **P9-02** Làm `GET /health`, danh sách sản phẩm phân trang và chi tiết sản phẩm theo hợp đồng.
- [ ] **P9-03** Làm `POST /api/v1/events`, kiểm tra schema, user, session, sản phẩm và cart. Chỉ xác nhận lưu thành công sau khi transaction commit.
- [ ] **P9-04** Trả đúng mã khi gửi lại cùng event và nội dung, hoặc cùng ID nhưng khác nội dung. Kiểm tra cả trường hợp request đồng thời.
- [ ] **P9-05** Làm API personalized với baseline fallback, substitutes cùng danh mục và complements từ tập giỏ gửi lên.
- [ ] **P9-06** Chốt giới hạn tham số, số lượng ứng viên và quy tắc tie-break. Trả đúng ID, rank, score, method và `run_id`.
- [ ] **P9-07** Xử lý chưa có snapshot, user mới, item mới, item không tồn tại, giỏ rỗng và danh sách ứng viên không đủ.
- [ ] **P9-08** Xuất hoặc kiểm tra OpenAPI và bổ sung ví dụ request, response chạy được vào [hướng dẫn vận hành](README.md).

Đầu ra là API đọc được snapshot thật và ghi được event thật. Cả hai người gọi cùng request trên cùng snapshot phải nhận cùng danh sách theo quy tắc đã chốt.

## P10. Website demo Streamlit

Phụ trách A. Phụ thuộc API fixture P9.

- [ ] **P10-01** Làm trang danh mục, chi tiết sản phẩm và giỏ hàng, có ảnh, tên, danh mục và trạng thái khi dữ liệu đang tải hoặc thiếu.
- [ ] **P10-02** Giữ user ID, session ID và cart ID trong state theo quy ước. Cho chọn một user demo có lịch sử để trình diễn ALS và tạo phiên mới để thử cold-start.
- [ ] **P10-03** Ghi view, click và add_to_cart tại đúng thao tác. Giữ nguyên event ID khi retry và kiểm tra rerun giao diện để tránh ghi thêm event ngoài ý muốn.
- [ ] **P10-04** Hiển thị Dành cho bạn ở trang chủ, Sản phẩm thay thế ở trang chi tiết và Thường thêm cùng ở giỏ hàng.
- [ ] **P10-05** Đồng bộ giỏ đang hiển thị với body gửi API complements, xử lý thêm sản phẩm trùng và tạo giỏ mới.
- [ ] **P10-06** Hiển thị trạng thái lỗi API, giỏ rỗng, ảnh lỗi và thiếu ứng viên theo cách người dùng demo hiểu được.

Đầu ra là website thao tác được bằng dữ liệu thật từ API, tạo log có thể đưa trở lại pipeline.

## P11. Gợi ý cập nhật theo phiên

Phụ trách A + B. B xác định cách lấy ứng viên và tính điểm, A tích hợp API và UI. Phụ thuộc P7-07, P9 và P10.

- [ ] **P11-01** Triển khai cửa sổ 30 phút/tối đa 20 event, trọng số 1/2/4, half-life 10 phút và kết hợp 0.7 batch + 0.3 session; xử lý late event theo [DATA_CONTRACTS.md](DATA_CONTRACTS.md).
- [ ] **P11-02** API chỉ đọc lịch sử của đúng user và session, lấy ứng viên từ danh sách tương tự đã tính sẵn rồi kết hợp ứng viên cá nhân hóa hoặc popularity từ batch.
- [ ] **P11-03** Chuẩn hóa và kết hợp điểm, khử trùng, lọc item đã tương tác và dùng fallback khi phiên thiếu tín hiệu hoặc thiếu ứng viên.
- [ ] **P11-04** Sau khi API xác nhận lưu sự kiện, UI yêu cầu gợi ý mới. Xử lý cache để kết quả có thể sử dụng sự kiện vừa commit.
- [ ] **P11-05** Trình diễn hai danh sách từ cùng snapshot, một theo batch và một có tín hiệu phiên. Hiển thị `run_id` và thời điểm lấy kết quả ở chế độ kiểm tra demo.
- [ ] **P11-06** Dùng kịch bản đổi từ danh mục A sang B để quan sát top 10 thay đổi khi giữ nguyên batch. Kiểm tra hai phiên hoạt động đồng thời được tách lịch sử.
- [ ] **P11-07** Đo từ lúc thao tác UI đến lúc danh sách mới hiển thị, ghi p50/p95 và chốt mục tiêu độ trễ từ số đo ban đầu.

Đầu ra là demo gợi ý thay đổi theo phiên và phép đo độ trễ. Mô hình dài hạn được cập nhật theo batch, tín hiệu phiên được áp dụng trong đường xử lý request.

## P12. Đánh giá chất lượng và hiệu năng

Phụ trách B đo mô hình và batch, A đo API và UI. Phụ thuộc các mô hình P7, API P9 và phần phiên P11.

- [ ] **P12-01** Chạy popularity, TF-IDF, ALS và phương án cá nhân hóa có MMR trên cùng bộ dữ liệu, mốc chia, tập ứng viên và chính sách lọc.
- [ ] **P12-02** Báo Recall@10, NDCG@10, đa dạng nội danh sách và độ phủ catalog, kèm số user và số nhãn thực sự được đánh giá.
- [ ] **P12-03** Đánh giá thay thế bằng ca cùng danh mục và kiểm tra mua kèm bằng giỏ giữ lại sản phẩm, ghi support, confidence và lift của các luật dùng.
- [ ] **P12-04** So sánh chỉ batch, chỉ tín hiệu phiên và kết hợp bằng dự đoán click tiếp theo. Chỉ dùng các sự kiện xảy ra trước thời điểm dự đoán.
- [ ] **P12-05** Phân tích user hoặc item mới, danh mục ít dữ liệu và các trường hợp fallback. Tách kết quả dữ liệu mô phỏng với demo khi đủ mẫu.
- [ ] **P12-06** Đo ETL, train, xuất top K, kích thước output và bộ nhớ nếu có thể, ở các mức dữ liệu đã chốt. Ghi CPU, RAM, hệ điều hành và cấu hình Spark.
- [ ] **P12-07** Đo độ trễ API p50/p95 sau warm-up, tỷ lệ lỗi và số request đồng thời. Đo riêng API đọc gợi ý và API nhận event.
- [ ] **P12-08** Đo tốc độ nhận click và độ trễ gợi ý theo phiên, theo dõi ảnh hưởng khi batch và demo cùng chạy trên một máy.
- [ ] **P12-09** Cố định cấu hình sau validation rồi chạy test cuối, lưu CSV hoặc JSON kết quả và biểu đồ liên kết với `dataset_version`, commit code và `run_id`.
- [ ] **P12-10** Phân tích các mức tăng hoặc giảm so với baseline, tác động của MMR và các giới hạn của dữ liệu mô phỏng dựa trên kết quả đã đo.

Đầu ra là bảng kết quả và biểu đồ dùng được trong báo cáo. Kết quả mô hình được so sánh bằng đúng tập dữ liệu và cách tính metric đã chốt.

## P13. Kiểm thử tích hợp và khả năng chạy lại

Phụ trách A + B. Viết các kiểm tra tương ứng cùng từng phần, nghiệm thu lại khi đã nối hệ thống.

- [ ] **P13-01** Kiểm tra schema, ID, duplicate, UTC, xử lý bản ghi lỗi và khả năng tái lập dữ liệu cùng seed bằng fixture nhỏ.
- [ ] **P13-02** Kiểm tra chống rò rỉ train/test, ánh xạ ID sau khi tải model, top K sau lọc và luật mua kèm nhiều antecedent.
- [ ] **P13-03** Kiểm tra event retry, request đồng thời, lỗi DB, batch lỗi, import lại và chuyển snapshot khi đang có request.
- [ ] **P13-04** Kiểm tra publish hoặc download dataset bị gián đoạn, file sai checksum, phiên bản không có và tài khoản thiếu quyền.
- [ ] **P13-05** Chạy một luồng tích hợp nhỏ từ sinh dữ liệu → ETL → train → import → API → UI → event mới → xuất log → batch tiếp theo.
- [ ] **P13-06** Chạy Ruff, kiểm tra định dạng và bộ test liên quan. Sửa các lỗi ảnh hưởng tới đầu ra hoặc khả năng tái lập.
- [ ] **P13-07** Đổi người thực hiện, tạo checkout và môi trường sạch trên máy teammate, chạy đúng hướng dẫn bằng đường dẫn và tài khoản của máy đó.

Đầu ra là các lệnh kiểm tra chạy được và bằng chứng luồng tích hợp đạt trên hai máy.

## P14. Báo cáo, sơ đồ và slide

Phụ trách A + B. Tài liệu đầu ra đặt trong `docs/`. Điền dần khi các phần triển khai hoàn tất.

- [ ] **P14-01** Hoàn thiện Chương I về bài toán, ba nhóm gợi ý, dữ liệu, nguồn thu thập, hành vi mô phỏng và cơ sở Big Data, Data Lake, recommender.
- [ ] **P14-02** Hoàn thiện Chương II về kiến trúc, Data Lake ba lớp, schema, ETL, thuật toán, API, giao diện và luồng theo phiên.
- [ ] **P14-03** Hoàn thiện Chương III về môi trường, cách chạy, thiết kế thực nghiệm, kết quả chất lượng, hiệu năng, phân tích và giới hạn.
- [ ] **P14-04** Lưu ERD và sơ đồ tech stack đọc rõ được trong `docs/`, đối chiếu với migration và code cuối cùng.
- [ ] **P14-05** Chuẩn bị bảng và biểu đồ chất lượng, hiệu năng, quy mô dữ liệu, kèm nguồn số liệu và cấu hình thí nghiệm.
- [ ] **P14-06** Chụp các màn hình ba nhóm gợi ý và so sánh batch với phiên, dùng cùng dữ liệu demo có thể chạy lại.
- [ ] **P14-07** Hoàn thiện nguồn tham khảo, mô tả nguồn catalog và ghi rõ tỷ lệ dữ liệu mô phỏng, giả định sinh dữ liệu và giới hạn suy rộng.
- [ ] **P14-08** Làm slide bám theo bài toán, kiến trúc, demo, kết quả thực nghiệm và kết luận từ số liệu.
- [ ] **P14-09** Chia phần trình bày, chuẩn bị trả lời về đầu ra train, cách đưa model vào website, cold-start, MMR, mua kèm và luồng theo phiên.

Đầu ra là báo cáo, slide và các hình dùng trong bài. Người review đối chiếu số liệu báo cáo với output thí nghiệm đã lưu.

## P15. Demo và bàn giao cuối

Phụ trách A + B. Phụ thuộc P13 và P14.

- [ ] **P15-01** Chốt một dataset, cấu hình, model và snapshot demo, lưu version hoặc checksum để tải và khôi phục khi cần.
- [ ] **P15-02** Ghi chính xác thứ tự lệnh cài môi trường, migrate DB, lấy dataset, ETL, train, import snapshot, chạy API và Streamlit vào [hướng dẫn vận hành](README.md).
- [ ] **P15-03** Demo user đã có lịch sử nhận gợi ý cá nhân hóa và user mới nhận fallback, đối chiếu với snapshot đã chọn.
- [ ] **P15-04** Demo sản phẩm thay thế, giỏ có mua kèm, giỏ rỗng và trường hợp thông số không tương thích khi có dữ liệu.
- [ ] **P15-05** Demo click mới cập nhật gợi ý theo phiên, sau đó xuất event và chạy lại batch để chứng minh vòng lặp dữ liệu.
- [ ] **P15-06** Chạy lại toàn bộ kịch bản trên máy trình bày và chuẩn bị bản dataset, snapshot local để demo khi kết nối kho chia sẻ gián đoạn.
- [ ] **P15-07** Review toàn bộ thay đổi, merge các PR đã duyệt, gắn phiên bản code cuối và kiểm tra teammate truy cập được code, dataset, model.
- [ ] **P15-08** Đóng gói báo cáo, slide và các đầu ra giảng viên yêu cầu trong `docs/`, kiểm tra tên nhóm, thành viên, hình, bảng và các liên kết trước khi nộp.
- [ ] **P15-09** Cả hai đối chiếu checklist với yêu cầu đề cương và ghi lại issue còn tồn tại cùng ảnh hưởng thực tế.

## Thứ tự tích hợp để hai người bắt đầu song song

| Mốc | Việc A bàn giao | Việc B bàn giao | Nghiệm thu |
| --- | --- | --- | --- |
| M0 | Catalog, event fixture và cấu hình máy API | Code đọc fixture và schema Silver | Hai máy dùng cùng ID, schema và UTC. |
| M1 | Bộ dữ liệu nhỏ có version và bước upload | Bước tải, ETL, popularity và output Gold nhỏ | Teammate lấy dataset và xuất top K. |
| M2 | Migration, importer, API và UI cơ bản | Snapshot popularity và lịch sử lọc item | Trang chủ hiển thị gợi ý, thao tác UI ghi được event. |
| M3 | UI đủ ba nhóm và tích hợp mua kèm | TF-IDF, ALS, FP-Growth, MMR và metric | Ba nhóm gợi ý hoạt động với output thật. |
| M4 | API đọc event phiên và màn hình so sánh | Cách xếp hạng theo phiên và đánh giá tiếp theo | Click mới làm top 10 đổi với cùng batch. |
| M5 | Số đo API, demo tái lập và tài liệu vận hành | Số đo batch, chất lượng và phân tích | Checkout sạch chạy được, báo cáo và slide hoàn chỉnh. |

## Điều kiện hoàn thành dự án

- [ ] Code sinh hoặc thu thập dữ liệu, pipeline, model, API và UI chạy được theo tài liệu trên máy của cả hai người.
- [ ] Bạn công bố dataset và teammate tải, train bằng luồng tự động có phiên bản, manifest và kiểm tra toàn vẹn.
- [ ] Model lưu và tải lại được, kết quả train được đưa vào PostgreSQL và hiển thị trong cả ba nhóm gợi ý trên website.
- [ ] Gợi ý theo phiên sử dụng được tương tác vừa ghi và có phép đo độ trễ.
- [ ] Có so sánh với baseline, phân chia theo thời gian, số liệu chất lượng, đa dạng, độ phủ và hiệu năng theo quy mô.
- [ ] Có demo hoàn chỉnh, báo cáo, slide, sơ đồ và kết quả thực nghiệm đối chiếu được với code và dữ liệu cuối cùng.
