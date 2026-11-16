# Hợp đồng dữ liệu, PostgreSQL và API v1

[Trang chủ repo](../README.md) · [Tech Stack](TECH_STACK.md) · [Quy ước đồng nhất](CONVENTIONS.md) · [Vận hành](README.md) · [Kiến trúc](ARCHITECTURE.md) · [Thực nghiệm](EXPERIMENT_PLAN.md) · [Kế hoạch & Phân công](../PLAN.md) · [Checklist](PROJECT_CHECKLIST.md)

## 1. Phạm vi và quy ước chung

Đây là nguồn đặc tả chuẩn cho schema/fixture T02, bộ sinh T03, ETL T04, ranking T05–T07 và PostgreSQL/API T08–T10 trong [PLAN.md](../PLAN.md). Hợp đồng có phiên bản **v1**, ghi `schema_version=1` ở manifest; không thêm trường version vào từng event. T01 cung cấp đặc tả, chưa cung cấp implementation.

| Quy ước | Giá trị v1 |
| --- | --- |
| Encoding | UTF-8, LF; JSONL có một object hợp lệ mỗi dòng, không có dòng trống. |
| ID nghiệp vụ | Chuỗi không rỗng, tối đa 128 ký tự Unicode; phân biệt hoa/thường, không chứa ký tự điều khiển hoặc khoảng trắng đầu/cuối. Không ép ID số thành chuỗi hoặc tự cắt khoảng trắng. |
| `dataset_version`, `run_id` | ID dùng trong đường dẫn, khớp `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`; không chứa dấu phân cách thư mục. |
| `product_id` | `<source>:<id_goc>`, ổn định qua các lần lấy catalog; `source` không chứa dấu `:`. |
| `event_id` | UUID hợp lệ; khi xuất dùng chữ thường, dạng có dấu gạch nối. Bộ sinh tạo xác định theo seed; UI tạo UUID một lần cho mỗi thao tác. |
| Thời gian wire | ISO 8601 UTC, nhận hậu tố `Z` hoặc `+00:00`; từ chối timestamp thiếu timezone hoặc offset khác UTC. Xuất hậu tố `Z`, độ chính xác tối đa microsecond. |
| Thời gian nội bộ | Spark `TimestampType` với timezone session UTC; PostgreSQL `TIMESTAMPTZ`, connection timezone UTC. |
| Số | Cấm NaN, `Infinity`, `-Infinity`; số nguyên không nhận boolean hoặc số thực có phần lẻ. |
| Trọng số hành vi | `view=1`, `click=2`, `add_to_cart=4`, cùng giá trị trong bộ sinh, ETL, hồ sơ user và tín hiệu phiên. |
| Null và trường thiếu | Chỉ các trường ghi nullable mới nhận null; trường tùy chọn dùng default được nêu rõ. Trường JSON không có trong schema bị từ chối. |
| Thứ tự khi bằng điểm | ID tăng dần theo thứ tự Unicode; PostgreSQL dùng collation `C` cho các ID dùng tie-break, Spark/Python dùng thứ tự chuỗi tương ứng. |

ID user/session/cart là ID giả danh, không đòi bảng tài khoản. Một `session_id` thuộc đúng một user; một `cart_id` thuộc đúng một `(user_id, session_id)` và duy nhất toàn cục. Phiên hoặc giỏ mới phải tạo ID mới. Kiểm tra quan hệ này trên cả API và dữ liệu mô phỏng; request cạnh tranh phải được tuần tự hóa khi xác lập chủ sở hữu session/cart.

## 2. Bronze JSONL

### 2.1. Sản phẩm

Vị trí: `data/bronze/products/`, chia theo đợt thu thập. Sự tồn tại trong catalog không có nghĩa là đã từng có tương tác.

| Trường | Kiểu JSON | Nullable/default | Ràng buộc |
| --- | --- | --- | --- |
| `product_id` | string | Bắt buộc | ID theo quy tắc mục 1. |
| `name` | string | Bắt buộc | Không rỗng sau kiểm tra khoảng trắng, tối đa 512 ký tự. |
| `category_id` | string | Bắt buộc | ID danh mục ổn định, tối đa 128 ký tự. |
| `source` | string | Bắt buộc | Nhãn nguồn catalog, không rỗng, tối đa 64 ký tự; khớp prefix của `product_id`. |
| `collected_at` | string UTC | Bắt buộc | Thời điểm thu thập phiên bản metadata này. |
| `description` | string | null | Tối đa 20.000 ký tự; chuỗi chỉ chứa khoảng trắng chuẩn hóa thành null. |
| `brand` | string | null | Tối đa 256 ký tự; chuỗi trắng thành null. |
| `image_url` | string | null | URL tuyệt đối HTTP/HTTPS, tối đa 2.048 ký tự. |
| `product_url` | string | null | URL tuyệt đối HTTP/HTTPS, tối đa 2.048 ký tự. |
| `attributes` | object string → string | `{}`, không nullable | Khóa không rỗng, không có giá trị null, không lồng object/list. Dùng để kiểm tra thông số tương thích đã chuẩn hóa. |

Một dòng ví dụ hoàn chỉnh, hợp lệ mà không cần dịch vụ ngoài:

```json
{"product_id":"fixture:p001","name":"Bàn phím cơ USB-C","category_id":"keyboard","source":"fixture","collected_at":"2026-10-01T00:00:00Z","description":"Bàn phím cơ có dây, bố cục 75 phần trăm","brand":"Demo","image_url":null,"product_url":null,"attributes":{"connector":"usb-c","layout":"75"}}
```

Giữ các phiên bản metadata khác thời điểm. Cùng `(product_id, collected_at)` và cùng payload chuẩn hóa là duplicate; khác payload là conflict. Không dùng thứ tự file để quyết định bản ghi thắng.

### 2.2. Sự kiện

Vị trí: `data/bronze/events/`, gồm dữ liệu mô phỏng và log demo được xuất từ DB.

| Trường | Kiểu JSON | Nullable/default | Ràng buộc |
| --- | --- | --- | --- |
| `event_id` | string UUID | Bắt buộc | Khóa bất biến của một thao tác logic. |
| `user_id` | string | Bắt buộc | Theo quy tắc ID. |
| `session_id` | string | Bắt buộc | Thuộc đúng user đã gửi. |
| `product_id` | string | Bắt buộc | Tham chiếu catalog. |
| `event_type` | string enum | Bắt buộc | `view`, `click`, `add_to_cart`. |
| `event_at` | string UTC | Bắt buộc | Thời điểm hành vi, không phải thời điểm nhận request. |
| `source` | string enum | Bắt buộc | `synthetic` hoặc `demo`; endpoint ghi event chỉ nhận `demo`. |
| `cart_id` | string | null | Bắt buộc khác null với `add_to_cart`; với hai loại còn lại có thể null hoặc là giỏ hợp lệ cùng phiên. |

```json
{"event_id":"ed8f2fa8-9b81-43b6-8bd4-9b8115640866","user_id":"u_demo_001","session_id":"s_demo_001","product_id":"fixture:p001","event_type":"add_to_cart","event_at":"2026-11-10T04:00:00Z","source":"demo","cart_id":"c_demo_001"}
```

API không nhận `received_at`, `catalog_run_id` hoặc `interaction_weight` từ client. Exporter chỉ xuất tám trường trên, giữ nguyên UUID và payload nghiệp vụ khi xuất lại. `event_at` quá cũ không bị tự đổi thành giờ nhận; event có thời điểm tương lai không được dùng trong cửa sổ phiên cho tới khi thời điểm đó đến.

### 2.3. Manifest và công bố dataset

Manifest là object JSON có các trường sau. Một phiên bản chỉ được công bố sau khi mọi file được ghi xong và SHA-256 tính từ byte thực tế; không dùng checksum minh họa thay checksum thật.

| Trường | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `schema_version` | integer | Luôn là `1`. |
| `dataset_version` | string ID | Định danh bất biến của gói đầu vào, không dùng lại cho nội dung khác. |
| `created_at` | string UTC | Thời điểm hoàn tất gói. |
| `code_commit` | string | Git commit tạo gói; lưu thêm `code_dirty` boolean nếu checkout có thay đổi chưa commit. |
| `generator_version` | string hoặc null | Phiên bản bộ sinh; null khi chỉ chứa dữ liệu thu thập/demo. |
| `catalog_version` | string ID | Phiên bản catalog dùng kiểm tra khóa ngoại. |
| `seed` | integer hoặc null | Seed mô phỏng, mặc định thí nghiệm `42`; null nếu không sinh dữ liệu. |
| `parameters` | object | Số user/phiên/event yêu cầu, khoảng thời gian và cấu hình phân bố thực dùng. |
| `counts` | object | Số product ID, phiên bản catalog, user, session, cart, event và số event theo source/type. |
| `event_time_min`, `event_time_max` | string UTC hoặc null | Min/max thực tế, null khi không có event. |
| `files` | array object | Mỗi phần tử có `path`, `format` (`jsonl`, `parquet`, `json`, `binary`), `rows`, `bytes`, `sha256` (64 ký tự hex chữ thường). `rows` là integer >= 0 với JSONL/Parquet, null với JSON/binary; `bytes` là integer >= 0. |

`path` tương đối với thư mục chứa manifest, dùng `/`, không có đường dẫn tuyệt đối hoặc `..`. Parquet liệt kê từng data file; `rows` là số dòng trong chính file đó, không tính `_SUCCESS`. Tổng số dòng được đối chiếu với thống kê bảng. Dataset Silver bổ sung `quality_report` (đường dẫn JSON nằm trong danh sách file kiểm tra toàn vẹn) chứa số dòng input, accepted, duplicate và rejected theo lý do. Đợt export demo thêm `exported_at` và `received_at_max` để chẩn đoán phạm vi dữ liệu, không coi chúng là watermark bảo đảm mọi transaction cũ đã commit.

### 2.4. Hồ sơ người dùng mô phỏng

Vị trí: `data/bronze/users/users.jsonl`; seed JSON array tại
`data/raw/users_1000.json`. `UserProfile` kế thừa `ContractModel`: strict,
forbid extra, không ép kiểu primitive. Profiles là dữ liệu giả lập phục vụ bộ sinh,
không bổ sung bảng tài khoản hoặc thay đổi payload Event/API v1.

| Trường | Kiểu JSON | Ràng buộc |
| --- | --- | --- |
| `user_id` | string | BusinessId, duy nhất trong tập profiles. |
| `name` | string | 1–128 ký tự; Faker locale `vi_VN`. |
| `gender` | string enum | `male`, `female`, `other`. |
| `age` | integer | 13–100; từ chối boolean, float và chuỗi số. |
| `city` | string | Bộ sinh chọn tên thành phố Việt Nam. |
| `primary_interest` | string | BusinessId tham chiếu category_id. |
| `secondary_interest` | string hoặc null | Default null; nếu có phải tham chiếu category_id. |
| `created_at` | string UTC | Nhận `Z`/`+00:00`, xuất `Z`. |

Schema kiểm tra từng bản ghi; loader kiểm tra user_id trùng và khóa ngoại sở
thích với catalog được truyền vào. Bộ sinh dùng 12 mã ngành hàng trong
`ingestion/catalog.py`, cân bằng sở thích chính và chọn sở thích phụ khác chính.
Nhân khẩu học được sinh độc lập với sở thích. Cùng seed và phiên bản Faker tạo
cùng nội dung seed/JSONL; thời gian công bố manifest vẫn là thời gian thực.

Consumer Kafka ghi từng micro-batch vào thư mục riêng, JSONL giữ nguyên tám
trường Event. Manifest có `source="kafka"`, `seed=null` (consumer không biết seed
của producer), `parameters.topic` và `catalog_validation="deferred_to_etl"`.
Consumer không nạp catalog: catalog_version/counts của catalog rỗng là giá trị
chẩn đoán; ETL phải gắn batch với catalog thật và kiểm tra khóa ngoại trước Silver.
File local được hoàn tất trước khi upload MinIO; manifest remote được upload sau
JSONL. Chỉ commit offset các event đã lưu bền vững ở local, kể cả khi MinIO lỗi.
Replay có thể tạo batch trùng; dedup theo event_id ở Silver giữ nguyên yêu cầu v1.

## 3. Silver Parquet

| Bảng và khóa logic | Trường Spark | Nullable |
| --- | --- | --- |
| `products`, khóa `(product_id, collected_at)` | `product_id: StringType`, `name: StringType`, `category_id: StringType`, `source: StringType`, `collected_at: TimestampType` | Không |
| `products`, trường mô tả | `description: StringType`, `brand: StringType`, `image_url: StringType`, `product_url: StringType` | Có |
| `products.attributes` | `MapType(StringType, StringType, valueContainsNull=False)` | Không; default map rỗng |
| `events`, khóa `event_id` | `event_id: StringType`, `user_id: StringType`, `session_id: StringType`, `product_id: StringType`, `event_type: StringType`, `event_at: TimestampType`, `source: StringType` | Không |
| `events.cart_id` | `StringType` | Có, trừ `add_to_cart` |

Không thêm weight vào Silver; đó là thuộc tính tổng hợp của cửa sổ học. Silver giữ đủ sự kiện hợp lệ theo thời gian để có thể chia lại mà không tạo rò rỉ. Ràng buộc nội dung giữ nguyên từ Bronze. Manifest ràng buộc một cặp products/events cùng `dataset_version`.

ETL phân loại `invalid_json`, `invalid_schema`, `invalid_timestamp`, `unknown_product`, `invalid_cart_owner`, `duplicate`, `conflicting_event_id`, `conflicting_product_version`. Duplicate giống hệt được gộp. Nhóm conflict bị cách ly toàn bộ; bản ghi không hợp lệ bị loại khỏi Silver và có báo cáo. Chỉ phát hành dataset sau khi người phụ trách xử lý conflict, không chọn tùy tiện “dòng đầu tiên”.

Silver products giữ lịch sử metadata. Với cutoff `t`, chọn mỗi sản phẩm bản có `collected_at < t` lớn nhất. Việc ID tồn tại ở một thời điểm tương lai trong dataset không khiến nó đủ điều kiện làm ứng viên ở cutoff trước đó. Chính sách chia tập và loại giỏ qua biên nằm trong [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md).

## 4. Gold Parquet và manifest run

Vị trí: `data/gold/<run_id>/`. Mỗi bảng là thư mục Parquet riêng, cột liệt kê dưới đây không nullable trừ khi ghi khác. Mọi `run_id` trong một snapshot phải bằng ID thư mục và manifest.

### 4.1. Tổng hợp học và catalog phục vụ

| Bảng | Cột và kiểu Spark | Khóa và ràng buộc |
| --- | --- | --- |
| `interactions` | `run_id: string`, `user_id: string`, `product_id: string`, `interaction_weight: double`, `last_event_at: timestamp` | PK logic `(run_id, user_id, product_id)`; weight hữu hạn > 0; timestamp lớn nhất của cặp trong cửa sổ. |
| `baskets` | `run_id: string`, `cart_id: string`, `user_id: string`, `session_id: string`, `product_ids: array<string>`, `first_event_at: timestamp`, `last_event_at: timestamp` | PK `(run_id, cart_id)`; mảng không null, item không null, duy nhất, sắp tăng, ít nhất một item; chỉ tổng hợp `add_to_cart`. |
| `products` | `run_id: string` và toàn bộ cột Silver products | PK `(run_id, product_id)`; chọn một phiên bản metadata as-of cutoff; nullable như Silver. |
| `seen_products` | `run_id: string`, `user_id: string`, `product_id: string` | PK ba cột; tất cả loại hành vi đều làm item trở thành seen. Chỉ xuất product còn trong catalog run. |

`interaction_weight(u,i) = n_view + 2 × n_click + 4 × n_add_to_cart`, tính sau dedup và chia tập. Giỏ một item vẫn có thể lưu để thống kê nhưng không dùng fit FP-Growth; mẫu số support của FP-Growth là số giỏ train có ít nhất hai item thực sự được đưa vào fit. Run manifest phải ghi số giỏ bị loại.

### 4.2. `recommendation_items`

| Cột | Kiểu Spark | Quy tắc |
| --- | --- | --- |
| `run_id` | string | Run chứa catalog, model và output. |
| `kind` | string enum | Chỉ `personalized`, `substitute`, `popular`. Endpoint tên số nhiều `substitutes`, giá trị kind dùng số ít `substitute`. |
| `context_id` | string | User ID nếu personalized; product ID gốc nếu substitute; chính xác `global` nếu popular. |
| `product_id` | string | Phải thuộc catalog cùng run. |
| `rank` | integer 32-bit | Liên tiếp từ 1 trong `(run_id, kind, context_id)`. |
| `score` | double | Hữu hạn; ý nghĩa phụ thuộc method. |
| `method` | string enum | `popularity`, `tfidf`, `als`, `als_mmr`; không lưu điểm session vào snapshot batch. |

Khóa duy nhất `(run_id, kind, context_id, product_id)` và `(run_id, kind, context_id, rank)`. Một run phục vụ chọn một phương án personalized chính; so sánh các phương án khác dùng run riêng cùng split. Popular chỉ dùng method popularity; substitute chỉ dùng tfidf; personalized có thể chứa popularity để bù thiếu nhưng giữ thứ tự sau các ứng viên chính.

Xuất tối đa 200 ứng viên mỗi context sau lọc, đủ lớn hơn K mặc định 10. Cho phép ít hơn nếu catalog hoặc mô hình không đủ. `popular` không lọc theo một user ở bước xuất; API lọc seen của user khi dùng fallback. `substitute` cùng danh mục và không chứa context gốc. Rank Gold giữ nguyên trong dữ liệu; API đánh lại rank sau lọc theo request.

```json
{"run_id":"run_20261108T000000Z_als_42","kind":"substitute","context_id":"fixture:p001","product_id":"fixture:p002","rank":1,"score":0.82,"method":"tfidf"}
```

### 4.3. `association_rules`

| Cột | Kiểu Spark | Quy tắc |
| --- | --- | --- |
| `run_id` | string | Cùng run snapshot. |
| `antecedent_ids` | array<string>, không null item | Tập không rỗng, duy nhất, sắp tăng; toàn bộ ID thuộc catalog run. |
| `consequent_id` | string | Một sản phẩm thuộc catalog run, không nằm trong antecedent. |
| `support` | double | `0 < support <= 1`, support của hợp antecedent và consequent. |
| `confidence` | double | `0 < confidence <= 1`. |
| `lift` | double | Hữu hạn > 0. |

Khóa `(run_id, antecedent_ids, consequent_id)`. Với tập giỏ train `B`, `support(X)=count(b chứa X)/|B|`, `confidence(A→i)=support(A∪{i})/support(A)`, `lift(A→i)=confidence(A→i)/support({i})`. Nếu thư viện trả mảng consequent, tách từng item và tính lại metric cho đúng luật một consequent, không sao chép metric của một luật nhiều consequent.

```json
{"run_id":"run_20261108T000000Z_als_42","antecedent_ids":["fixture:p001"],"consequent_id":"fixture:p010","support":0.12,"confidence":0.6,"lift":1.5}
```

### 4.4. Model, ánh xạ và manifest

Model ALS lưu cùng bảng `user_id: string, user_index: integer` và `product_id: string, product_index: integer`; mỗi phía là song ánh, index trong `[0, 2147483647]`, cấp theo ID tăng dần để tái lập. Vectorizer, IDF, vocabulary, quy tắc tiền xử lý và feature vector theo product được lưu theo run. Model tải lại phải dùng đúng ánh xạ đó.

Manifest Gold có `schema_version`, `run_id`, `dataset_version`, `created_at`, `code_commit`, `code_dirty`, `seed`, `training_start`, `training_end` (exclusive), `evaluation_window` (object hoặc null), `parameters`, `environment`, `counts`, `files`. `files` theo schema ở mục 2.3 và bổ sung `table` cho file Parquet. `parameters` chứa weights, model/hyperparameter, candidate limit, quy tắc lọc và tie-break. `environment` có Python, Java, Spark, OS và cấu hình Spark thực dùng.

Bốn bảng phục vụ bắt buộc: `products`, `recommendation_items`, `seen_products`, `association_rules`; bảng rỗng phải có schema, số dòng 0 được ghi trong `counts`. Model và ánh xạ bắt buộc khi run dùng thuật toán tương ứng. Fingerprint run là SHA-256 của byte `manifest.json`; manifest không chứa checksum của chính nó. Cùng run ID phải có cùng fingerprint; nội dung mới phải tạo run mới.

## 5. PostgreSQL: bảy bảng phục vụ

Đây là đặc tả cho migration T08. Mọi cột mặc định `NOT NULL` trừ cột ghi `NULL`. ID dùng `TEXT COLLATE "C"` và ràng buộc độ dài mục 1. Timestamp dùng `TIMESTAMPTZ`. FK mặc định `ON DELETE RESTRICT`; không cascade xóa lịch sử khi đổi run.

### 5.1. `batch_runs`

| Cột | Kiểu PostgreSQL | Ràng buộc/default |
| --- | --- | --- |
| `run_id` | TEXT | PK. |
| `dataset_version` | TEXT | Đầu vào cố định của run. |
| `schema_version` | SMALLINT | CHECK bằng 1. |
| `manifest_sha256` | CHAR(64) | Hex chữ thường, fingerprint kiểm tra retry. |
| `status` | TEXT | CHECK trong `loading`, `ready`, `failed`. |
| `training_start`, `training_end` | TIMESTAMPTZ | `training_start < training_end`. |
| `seed` | BIGINT | Seed thực dùng. |
| `code_commit` | TEXT | Commit được ghi trong manifest. |
| `parameters`, `environment`, `row_counts` | JSONB | Mỗi cột là JSON object, theo manifest. `parameters` lưu cả `code_dirty`. |
| `created_at` | TIMESTAMPTZ | DEFAULT `CURRENT_TIMESTAMP`. |
| `activated_at` | TIMESTAMPTZ NULL | Lần kích hoạt gần nhất, null nếu chưa từng active. |
| `error_message` | TEXT NULL | Lý do thất bại, không chứa secret. |

### 5.2. `serving_state`

| Cột | Kiểu | Ràng buộc/default |
| --- | --- | --- |
| `singleton_id` | SMALLINT | PK, CHECK bằng 1. Migration tạo sẵn đúng một dòng. |
| `active_run_id` | TEXT NULL | FK `batch_runs(run_id)`; null khi chưa phát hành. |
| `updated_at` | TIMESTAMPTZ | DEFAULT `CURRENT_TIMESTAMP`; cập nhật khi đổi con trỏ. |

Importer chỉ được trỏ tới run `ready` đã có đủ bảng. Khóa singleton trước khi nạp/kích hoạt; readers không khóa. Retry run ready là no-op; muốn rollback phải yêu cầu kích hoạt run cũ một cách tường minh. Thuật toán transaction và kiểm tra cạnh tranh ở [ARCHITECTURE.md](ARCHITECTURE.md).

### 5.3. `products`

| Cột | Kiểu | Ràng buộc |
| --- | --- | --- |
| `run_id` | TEXT | FK batch_runs. |
| `product_id`, `category_id`, `source` | TEXT | Quy tắc ID/source; `product_id` khớp prefix source. |
| `name` | TEXT | Không rỗng, tối đa 512 ký tự. |
| `collected_at` | TIMESTAMPTZ | Phiên bản metadata trước cutoff. |
| `description`, `brand`, `image_url`, `product_url` | TEXT NULL | Quy tắc độ dài/URL theo Bronze. |
| `attributes` | JSONB | DEFAULT `'{}'::jsonb`, object có giá trị chuỗi. |

PK `(run_id, product_id)`. Index `(run_id, category_id, product_id)` phục vụ catalog và lọc substitutes. Không dùng chỉ `product_id` làm PK vì sẽ mất catalog của snapshot cũ.

### 5.4. `events`

| Cột | Kiểu | Ràng buộc |
| --- | --- | --- |
| `event_id` | UUID | PK, bất biến. |
| `user_id`, `session_id`, `product_id` | TEXT | ID hợp lệ. |
| `event_type` | TEXT | CHECK `view`, `click`, `add_to_cart`. |
| `event_at` | TIMESTAMPTZ | Thời điểm client ghi nhận hành vi. |
| `source` | TEXT | CHECK bằng `demo`; dữ liệu synthetic đi vào Bronze. |
| `cart_id` | TEXT NULL | CHECK `event_type <> 'add_to_cart' OR cart_id IS NOT NULL`; nếu có thì ID hợp lệ. |
| `received_at` | TIMESTAMPTZ | DEFAULT `clock_timestamp()`, server cấp khi nhận event mới; retry không cập nhật. |
| `catalog_run_id` | TEXT | FK kép `(catalog_run_id, product_id)` → `products(run_id, product_id)`. |

Index `(user_id, session_id, event_at DESC, received_at DESC, event_id)` cho tín hiệu phiên; `(received_at, event_id)` cho xuất/chẩn đoán; `(cart_id, user_id, session_id)` và `(session_id, user_id)` cho kiểm tra chủ sở hữu. API không cho UPDATE payload. Quan hệ một session/cart thuộc một user phải được kiểm tra trong transaction, dùng advisory transaction lock theo session/cart theo thứ tự khóa cố định để tránh race.

### 5.5. `recommendation_items`

Cột `run_id`, `kind`, `context_id`, `product_id`, `method` là TEXT; `rank` INTEGER CHECK > 0; `score` DOUBLE PRECISION hữu hạn. PK `(run_id, kind, context_id, product_id)`, UNIQUE `(run_id, kind, context_id, rank)`, FK run và FK kép product cùng run. Enum, method và ngữ nghĩa context theo mục 4.2. Context substitute phải tham chiếu product cùng run, kiểm tra bằng trigger hoặc importer có test tương ứng.

Index `(run_id, kind, context_id, rank)` hỗ trợ top K. Kiểm tra hữu hạn bằng CHECK chặn NaN và hai vô cực; không chỉ dùng `score IS NOT NULL`. Điều kiện `score > '-Infinity'::float8 AND score < 'Infinity'::float8` đáp ứng quy tắc này trên PostgreSQL.

### 5.6. `seen_products`

`run_id`, `user_id`, `product_id`: TEXT; PK cả ba cột; FK run và FK kép product cùng run. PK cũng là index truy vấn seen theo `(run_id, user_id)`. Chỉ chứa lịch sử trước cutoff của snapshot; event mới không sửa bảng này mà đi vào cửa sổ phiên hoặc batch tiếp theo.

### 5.7. `association_rules`

`run_id`: TEXT; `antecedent_ids`: TEXT[] không null; `consequent_id`: TEXT; `support`, `confidence`, `lift`: DOUBLE PRECISION không null. PK `(run_id, antecedent_ids, consequent_id)`, FK run và FK kép consequent cùng run. CHECK mảng một chiều, cardinality > 0, không null phần tử, consequent không thuộc antecedent; support/confidence trong `(0,1]`, lift hữu hạn > 0.

Trigger hoặc hàm CHECK của migration xác nhận mảng tăng dần và không trùng; importer kiểm tra từng antecedent thuộc catalog vì FK thông thường không áp dụng cho từng phần tử array. Index GIN `antecedent_ids` hỗ trợ toán tử subset, B-tree `(run_id, confidence DESC, lift DESC, consequent_id)` hỗ trợ thứ tự. Không yêu cầu giỏ input giống nguyên antecedent: `{a,b} ⊆ {a,b,c}` là khớp hợp lệ.

### 5.8. Import và đọc nhất quán

Bốn bảng phục vụ được kiểm tra trước, nạp trong cùng transaction với bước đổi con trỏ. Nếu lỗi, dữ liệu run mới và con trỏ rollback; trạng thái failed được ghi ở transaction riêng có điều kiện run vẫn loading cùng fingerprint, không ghi đè ready. API đọc nhiều bảng bằng một transaction `REPEATABLE READ`, lấy active run một lần. Import đồng thời phải khóa singleton, kiểm tra lại status/fingerprint và con trỏ chưa đổi so với lúc bắt đầu. Không xóa run/catalog đang active hoặc đang được events tham chiếu. Quy tắc isolation dựa trên [PostgreSQL 17](https://www.postgresql.org/docs/17/transaction-iso.html); đây là lựa chọn thiết kế của dự án.

## 6. Hợp đồng HTTP API v1

Base URL local `http://127.0.0.1:8000`, prefix `/api/v1`. Request/response JSON UTF-8. API không có authentication ở phạm vi demo local. `user_id` là định danh được UI chọn; không suy ra người dùng thật.

### 6.1. Kiểu response và mã lỗi

Object sản phẩm trả đúng các trường Bronze product, bao gồm optional đã điền null/map rỗng; không trả `run_id` trong từng product. Response gợi ý:

```json
{"run_id":"run_20261108T000000Z_als_42","items":[{"product_id":"fixture:p002","rank":1,"score":0.82,"method":"tfidf"}]}
```

`items` luôn là array, rank liên tiếp từ 1 sau lọc. Method ngoài các giá trị batch có thêm `session` và `fpgrowth`. Điểm có thể âm với ALS/MMR, không phải xác suất; tfidf/popularity/fpgrowth có miền mô tả ở mục 7. Không so trực tiếp điểm khác method để trộn fallback.

Mọi lỗi dùng envelope như `{"detail":{"code":"SNAPSHOT_UNAVAILABLE","message":"Chưa có snapshot phục vụ","fields":[]}}`; `fields` luôn là array, với lỗi validation mỗi phần tử có `path` (string) và `message` (string). V1 tùy biến handler validation của FastAPI để thống nhất envelope.

| HTTP | `detail.code` | Khi nào |
| --- | --- | --- |
| `404` | `PRODUCT_NOT_FOUND` | ID có cú pháp hợp lệ nhưng không thuộc catalog active; ghi rõ ID trong message. |
| `409` | `EVENT_ID_CONFLICT` | UUID đã tồn tại với payload nghiệp vụ khác. |
| `409` | `IDENTITY_CONFLICT` | Session/cart đã gắn user hoặc session khác. |
| `422` | `VALIDATION_ERROR` | Body, enum, thời gian, ID, giới hạn hoặc query sai schema. |
| `503` | `SNAPSHOT_UNAVAILABLE` | API dữ liệu chưa có active run. |
| `503` | `DATABASE_UNAVAILABLE` | DB lỗi kết nối/timeout hoặc không thể commit. |
| `500` | `INTERNAL_ERROR` | Lỗi nội bộ ngoài các trường hợp trên; message chung, chi tiết ghi server log. |

Với API dữ liệu: validate cú pháp trước; sau đó kiểm tra snapshot; rồi kiểm tra ID sản phẩm. Vì vậy giỏ sai schema trả 422, giỏ rỗng hợp lệ khi chưa có snapshot vẫn trả 503. Riêng retry event đã tồn tại được xử lý trước kiểm tra catalog hiện tại như mục 6.4. `/health` không kiểm tra DB.

### 6.2. `GET /health`

Không có body/query. Trả `200` và chính xác `{"status":"ok"}` khi tiến trình API hoạt động. Đây là liveness, không khẳng định DB hoặc snapshot sẵn sàng. Lỗi tham số không thuộc schema của route không thay đổi ý nghĩa liveness.

### 6.3. Catalog

| Route | Input | Thành công | Lỗi |
| --- | --- | --- | --- |
| `GET /api/v1/products` | `limit`: int default 20, 1–100; `offset`: int default 0, >= 0. | `200`, object có array `items` chứa product đầy đủ, thứ tự ID tăng dần; quá cuối trả `{"items":[]}`. | `422`, `503`. |
| `GET /api/v1/products/{product_id}` | ID trong path, URL-encode khi cần. | `200`, một object product đầy đủ. | `404`, `422`, `503`. |

Chỉ đọc products của active run. Ví dụ danh sách rỗng hợp lệ sau offset lớn là `{"items":[]}`. Khi UI đã giữ kết quả từ run cũ mà catalog vừa đổi, UI tải lại danh sách gợi ý; không coi 404 metadata là sản phẩm tương thích mới.

### 6.4. `POST /api/v1/events`: idempotency

Body đúng schema event mục 2.2, bắt buộc `source="demo"`. Không tự sinh UUID nếu client thiếu. Response tạo mới `201`, retry tương đương `200`; cả hai có cùng body:

```json
{"event_id":"ed8f2fa8-9b81-43b6-8bd4-9b8115640866"}
```

Thứ tự xử lý bắt buộc:

1. Validate đầy đủ body; chuẩn hóa UUID, datetime về UTC và `cart_id` thiếu thành null. So sánh payload theo giá trị chuẩn hóa, không theo thứ tự key hoặc byte JSON. `Z` và `+00:00` biểu diễn cùng thời điểm là tương đương.
2. Mở transaction, lấy advisory transaction lock theo UUID trước lookup để tuần tự hóa request cùng ID. Nếu `event_id` đã có: so sánh cả tám trường nghiệp vụ. Giống trả 200, khác trả 409. Không xét lại membership catalog để một retry hợp lệ vẫn thành công sau khi snapshot thay đổi. `received_at`/`catalog_run_id` không tham gia so sánh.
3. Với event mới: kiểm tra active run, sản phẩm tồn tại, quan hệ user–session–cart. Không có bảng users; user mới hợp lệ nếu ID đúng schema. ID sản phẩm không có trả 404, chưa có snapshot trả 503, sai chủ sở hữu trả 409.
4. INSERT dùng `ON CONFLICT (event_id) DO NOTHING` làm bảo vệ cuối ở DB; nếu không insert được thì đọc payload của bản ghi thắng sau khi transaction đó commit và so sánh lại. Event write dùng `READ COMMITTED` để câu SELECT tiếp theo nhìn thấy bản ghi thắng. Luôn lấy khóa event ID trước, rồi khóa session/cart theo thứ tự cố định; các khóa này bảo vệ kiểm tra chủ sở hữu giữa các UUID khác nhau.
5. Chỉ trả 201 sau commit thành công. DB không commit được trả 503; client retry cùng UUID để xác định kết quả sau mất kết nối. Không cập nhật payload hoặc received_at của bản ghi đã có.

Ví dụ conflict:

```json
{"detail":{"code":"EVENT_ID_CONFLICT","message":"event_id đã tồn tại với nội dung khác","fields":[]}}
```

### 6.5. `GET /api/v1/recommendations/personalized`

| Query | Kiểu/default | Ràng buộc |
| --- | --- | --- |
| `user_id` | string, bắt buộc | ID hợp lệ; user mới không phải lỗi. |
| `limit` | integer, default 10 | 1–50. |
| `session_id` | string hoặc không gửi | Nếu gửi phải là ID không rỗng; chỉ đọc đúng user và session. |

Không session: lấy personalized của user, lọc seen của snapshot và catalog; bù bằng popularity chưa trùng, chưa seen. Giữ thứ tự ứng viên chính rồi mới fallback; không xếp lại theo hai thang score khác nhau. User mới nhận popularity. Nếu session tồn tại dưới user khác trả 409 `IDENTITY_CONFLICT`; session chưa có event hợp lệ dùng batch. Có session và có tín hiệu thì áp dụng mục 7.3.

Thành công `200` với envelope gợi ý. `items=[]` hợp lệ nếu hết ứng viên. Lỗi `422`, `409`, `503`; không trả 404 cho user mới.

### 6.6. `GET /api/v1/recommendations/substitutes`

Query `product_id` bắt buộc, `limit` default 10, 1–50. Kiểm tra product tồn tại trong active run, lấy kind `substitute`, context bằng ID gốc; chỉ cùng category, loại item gốc và item không có trong catalog. Trả `200` với envelope và method `tfidf`; không có sản phẩm thay thế thì items rỗng. Lỗi `404`, `422`, `503`. Không bù bằng popularity khác danh mục.

### 6.7. `POST /api/v1/recommendations/complements`

```json
{"product_ids":["fixture:p001","fixture:p002"],"limit":10}
```

`product_ids` bắt buộc là array string, cho phép rỗng, tối đa 100 phần tử trước dedup; `limit` integer default 10, 1–50. Khử trùng ID thành tập và kiểm tra mọi ID tồn tại trong catalog active. Một ID không có làm cả request trả 404. Giỏ rỗng và snapshot sẵn sàng trả `200`, `items=[]`.

Khớp mọi luật có antecedent là tập con của giỏ. Với mỗi consequent chưa trong giỏ, chọn luật có confidence lớn nhất, rồi lift lớn nhất, rồi antecedent theo thứ tự từ điển để truy vết xác định. Lọc tương thích và catalog trước khi lấy top K; xếp kết quả `confidence DESC, lift DESC, product_id ASC`. Response method `fpgrowth`, score bằng confidence của luật đã chọn. Không có luật đủ điều kiện trả rỗng, không dùng popularity làm “mua kèm”. Lỗi `404`, `422`, `503`.

Các nhóm có quy tắc tương thích cần cấu hình rõ thuộc tính: ví dụ danh mục dây cáp và bàn phím có `connector`, chỉ cho phép khi connector bằng nhau. Khi cặp thuộc quy tắc nhưng một phía thiếu thuộc tính thì loại; cặp ngoài tập quy tắc chỉ được mô tả là đồng xuất hiện theo giỏ, không tuyên bố tương thích kỹ thuật.

## 7. Điểm số, lọc và tính xác định

### 7.1. Batch và fallback

Popularity score là tổng weight > 0. TF-IDF score là cosine trong `[0,1]`; văn bản ghép `name`, `category_id`, `description` (null thành chuỗi rỗng), lowercase và Unicode NFC. Fit vocabulary/IDF chỉ trên catalog as-of cutoff; dùng CountVectorizer + IDF của Spark để không thêm dependency ngoài kế hoạch. Hồ sơ user là tổng vector item có weight rồi L2-normalize; hồ sơ rỗng đi fallback.

ALS score là dự đoán hữu hạn, có thể âm. MMR score là điểm tại vòng chọn, có thể âm; rank là thứ tự chọn nên **không sort lại theo score MMR sau khi chọn**. Bỏ ứng viên không có catalog hoặc score không hữu hạn; loại seen trước khi lấy K. Với method thường, tie-break product ID tăng; với MMR, bằng objective thì product ID tăng.

### 7.2. Chuẩn hóa và MMR

Với một danh sách điểm hữu hạn, `norm(s)=(s-min)/(max-min)`. Nếu tất cả điểm bằng nhau và khác 0 thì norm bằng 1; nếu tất cả bằng 0 thì norm bằng 0. Ứng viên không có điểm ở một nguồn nhận 0, không được tham gia tính min/max của nguồn đó. Dùng công thức này cho relevance MMR và cho từng nguồn session/batch.

MMR chọn lần lượt `argmax[lambda × norm(relevance(i)) - (1-lambda) × max(cos(i,j), j thuộc danh sách đã chọn)]`; khi danh sách rỗng, phần phạt bằng 0. Vector thiếu hoặc norm bằng 0 dùng cosine 0 và ghi số lượng; việc tính metric đa dạng có chính sách riêng để tránh làm đẹp chỉ số do thiếu vector. Lambda được chọn trên validation theo [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md).

### 7.3. Điểm theo phiên

Đọc tối đa 20 event trong `[request_time - 30 phút, request_time]`, thứ tự `event_at DESC, received_at DESC, event_id ASC`. Chỉ dùng đúng user/session, đã commit. Với item ứng viên `i`, tính:

```text
session_score(i) = tổng trên event e:
    weight(e.event_type) × 2^(-age_minutes(e)/10) × similarity(e.product_id, i)
```

`similarity` lấy score TF-IDF từ bảng substitute cùng run, không khởi tạo vectorizer trong request. Item không có quan hệ tương tự nhận 0. Loại seen snapshot và mọi product trong cửa sổ phiên, khử trùng. Điểm batch nguồn chỉ dùng các row personalized có method khác popularity nếu có; nếu không có nguồn chính đó thì dùng popular. Các row popularity đã bù trong Gold không được chuẩn hóa chung với điểm ALS/TF-IDF; chúng chỉ bổ sung sau xếp hạng phần kết hợp.

Tính `combined=0.7 × norm(batch_score) + 0.3 × norm(session_score)` trên hợp ứng viên personalized/substitute (hoặc popular/substitute cho user mới). Chọn tối đa 200 theo combined rồi product ID; lấy limit và bù popularity còn hợp lệ nếu thiếu. Item từ combined có method `session`, score combined; item bù giữ `popularity` và score popularity. Không có tín hiệu dương hoặc không có ứng viên session sau lọc thì dùng batch nguyên bản. Không ghi điểm này vào Gold.

## 8. Điều kiện kiểm tra hợp đồng

| Nhóm | Ca bắt buộc từ các task triển khai |
| --- | --- |
| Schema | ID rỗng, UUID sai, timestamp thiếu timezone, enum sai, null sai chỗ, cart thiếu, unknown field. |
| ETL | Duplicate giống payload, conflict cùng ID, FK catalog thiếu, metadata phiên bản và báo cáo reject. |
| Gold | Khóa/rank trùng, rank hở, method/kind sai, NaN/vô cực, antecedent chưa chuẩn hóa, file/bảng thiếu. |
| Snapshot | Import lỗi giữa chừng, retry cùng fingerprint, đổi nội dung cùng run, hai importer, rollback cả catalog và kết quả. |
| API | Đủ 200/201/404/409/422/503, event đồng thời, retry sau đổi catalog, empty candidates, limit và giỏ rỗng. |
| Session | Event vừa commit ảnh hưởng request sau, late event ngoài cửa sổ không ảnh hưởng, tách hai phiên và hai user. |
| Replay | Xuất lại giữ ID, transaction commit muộn được lần xuất đầy đủ sau bao phủ, dedup không tăng weight lần hai. |

Hợp đồng thay đổi phải sửa tài liệu này trước hoặc cùng PR, cập nhật fixture/test và nêu tác động tới dataset/snapshot cũ. Người còn lại review theo [PLAN.md](../PLAN.md); việc đặc tả hoàn chỉnh không thay cho bằng chứng code chạy trong [PROJECT_CHECKLIST.md](PROJECT_CHECKLIST.md).
