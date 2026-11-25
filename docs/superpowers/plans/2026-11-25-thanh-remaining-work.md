# Kế hoạch hoàn thành phần việc của Nguyễn Đức Thành

> **Thực thi cho agent.** Dùng skill `superpowers:executing-plans` để thực hiện từng task trong kế hoạch này. Mỗi task có kiểm tra, cập nhật trạng thái, commit riêng và push GitHub theo yêu cầu của người dùng. Các bước dùng checkbox để theo dõi.

**Mục tiêu.** Hoàn thành phần sinh và bàn giao dữ liệu, PostgreSQL, FP-Growth, API, gợi ý theo phiên, xuất log, giao diện và phần tích hợp của Thành.

**Kiến trúc.** Tái sử dụng catalog, bộ sinh, schemas Pydantic, Kafka và MinIO đã có. Phần phục vụ đọc snapshot Gold có phiên bản từ PostgreSQL. Trong lúc chờ output của Pháp, dùng fixture Gold đúng hợp đồng để triển khai importer, API và UI.

**Tech stack.** Python 3.11, PostgreSQL 17, psycopg 3, FastAPI, Streamlit, PySpark 3.5.7, Kafka và MinIO. Đề xuất dùng PyArrow để importer đọc Parquet trực tiếp, bổ sung dependency khi thực hiện A03.

**Đặc tả.** [PLAN.md](../../../PLAN.md), [DATA_CONTRACTS.md](../../DATA_CONTRACTS.md), [ARCHITECTURE.md](../../ARCHITECTURE.md), [CONVENTIONS.md](../../CONVENTIONS.md), [EXPERIMENT_PLAN.md](../../EXPERIMENT_PLAN.md).

## 1. Phạm vi và hiện trạng

Ngày kiểm tra là 25/11/2026. Phân công theo tên lấy từ ma trận trong `PLAN.md`.

| Phần của Thành | Hiện trạng quan sát được | Task còn lại |
| --- | --- | --- |
| T03 catalog và sinh hành vi | Có code, 1.000 sản phẩm, 1.000 user, Bronze khoảng 119 nghìn sự kiện. | A00 hoàn thiện vận hành và A01 bàn giao dữ liệu. |
| T08 PostgreSQL và importer | Có đặc tả, module triển khai còn trống. | A02 và A03. |
| T06, phần FP-Growth | Chưa có implementation. | A04. |
| T09 API | Có kiểu request/response trong schemas, chưa có app. | A05 và A06. |
| T10 gợi ý theo phiên và exporter | Có công thức, chưa có implementation. | A07 và A08. |
| T11 Streamlit | Chưa có app. | A09. |
| T12 phần tích hợp, hiệu năng API và tài liệu | Chưa có kết quả tích hợp toàn hệ thống. | A10 và A11. |

T04 ETL, T05 popularity/TF-IDF, T06 ALS/MMR và T07 đánh giá mô hình do Pháp phụ trách theo kế hoạch hiện tại. Thành cần review hợp đồng và tích hợp các output này ở A10.

Kết quả kiểm tra gần nhất có 376 test pass sau khi cho phép truy cập HTTP localhost. Lint hiện có 8 lỗi trong `scripts/publish_catalog.py`, `scripts/run_stream_worker.py` và `ingestion/sync_minio.py`. Docker Engine chưa chạy tại thời điểm kiểm tra. Silver và Gold local đang trống.

Checkout còn thay đổi chưa commit trong bốn file và hai script chạy/dừng streaming mới. A00 phải đối chiếu diff với HEAD trước khi sửa và giữ các thay đổi phù hợp với yêu cầu vận hành.

## 2. Ràng buộc dùng chung

- Dùng lại `Product`, `Event`, `Basket`, `GoldProduct`, `AssociationRule`, các kiểu API và `get_settings()` hiện có.
- Giữ ID chuỗi, datetime UTC, `schema_version=1`, trọng số `view=1`, `click=2`, `add_to_cart=4` theo hợp đồng.
- Catalog PostgreSQL có khóa `(run_id, product_id)`. Log demo giữ `catalog_run_id` và `received_at` do server cấp.
- Import snapshot phải kiểm tra bốn bảng phục vụ, checksum, fingerprint và tính toàn vẹn trước khi kích hoạt.
- Request đọc dùng một transaction `REPEATABLE READ` và một `active_run_id`. Ghi event dùng `READ COMMITTED`, khóa idempotency và kiểm tra chủ sở hữu session/cart.
- Gợi ý phiên dùng tối đa 20 event trong 30 phút, half-life 10 phút, kết hợp `0.7 × batch + 0.3 × session`, tối đa 200 ứng viên.
- File tài liệu mới, ảnh demo, kết quả benchmark dùng trong báo cáo đặt dưới `docs/`. Code, test, migration và dữ liệu giữ đúng các thư mục chức năng của repo.
- Dữ liệu test DB dùng database kiểm thử riêng. Các lệnh dọn fixture chỉ tác động dữ liệu được tạo cho lượt test.

## 3. Các trường hợp phải được kiểm tra

1. Event được gửi lại sau đổi snapshot và request đồng thời cùng UUID hoặc cùng session phải giữ idempotency và chủ sở hữu.
2. Importer cạnh tranh hoặc lỗi giữa chừng phải giữ snapshot thành công, retry cùng fingerprint là no-op và rollback phải khôi phục đúng catalog.
3. Model chưa có luật, user mới, giỏ rỗng, metadata hoặc thuộc tính tương thích thiếu phải trả kết quả đúng hợp đồng.
4. UI rerun, đổi user và retry mạng phải giữ đúng user/session/cart và chỉ tạo event cho thao tác mới.
5. Log xuất lại, transaction commit muộn, MinIO lỗi và tải dữ liệu thiếu phải giữ được dữ liệu để xử lý lại.

Các test tương ứng được ghi trong task sở hữu hành vi đó bên dưới.

## 4. Quy trình commit và push sau mỗi task

Đích hiện tại là remote `origin`, repo `ndthanhh/big-data-product-recommender`. Nhánh triển khai dùng tiền tố `codex/` nếu tạo nhánh mới. Push nhánh hiện tại và kiểm tra SHA remote sau từng task. Bước merge phụ thuộc cách cộng tác trên repo và yêu cầu tiếp theo của người dùng.

Mỗi task thực hiện theo thứ tự sau.

1. Xem `git status --short` và diff để xác định phạm vi. Chỉ stage các file của task.
2. Với hành vi mới hoặc sửa lỗi, viết test cho trường hợp cụ thể, chạy để xác nhận failure rồi triển khai.
3. Chạy các lệnh nghiệm thu của task, Ruff cho file bị sửa và test hồi quy liên quan.
4. Kiểm tra đầu ra thực tế. Ghi trạng thái trong kế hoạch này, kèm lệnh đã chạy và hạn chế còn phụ thuộc dữ liệu của Pháp.
5. Chạy `git diff --check`, stage file tường minh và review `git diff --cached`.
6. Commit bằng message trong task, sau đó `git push -u origin HEAD` ở lần đầu hoặc `git push origin HEAD` ở các lần tiếp.
7. Đối chiếu `git rev-parse HEAD` với `git ls-remote origin refs/heads/<ten-nhanh>`. Chỉ ghi trạng thái đã push sau khi SHA khớp.

Nếu remote có commit mới, fetch, đọc thay đổi và tích hợp bằng merge hoặc rebase phù hợp với quyền sở hữu nhánh rồi chạy lại kiểm tra. Nếu push lỗi mạng, giữ commit local và retry. Trạng thái phải phân biệt code đã xong với commit đã lên remote.

## 5. Thứ tự thực hiện

`A00 → A01 → A02 → A03 → A04 → A05 → A06 → A07 → A08 → A09 → A10 → A11`.

Fixture Gold ở A03 giúp phát triển phần serving trước khi Pháp bàn giao model. A04 chạy bằng fixture baskets rồi tích hợp giỏ Gold thật ở A10. Các benchmark với mô hình thật và kết luận toàn hệ thống được thực hiện sau khi có output của Pháp.

## A00. Hoàn thiện các thay đổi streaming đang dở

**Liên quan T03.**

**File.** Sửa `scripts/run_stream_worker.py`, `scripts/start_continuous_stream.ps1`, `scripts/stop_continuous_stream.ps1`, `src/recommender/streaming/producer.py`, `src/recommender/streaming/consumer.py`, `src/recommender/ingestion/sync_minio.py` và `scripts/publish_catalog.py` theo lỗi đã xác minh. Test ở `tests/test_streaming.py`, `tests/test_stream_worker.py`. Cập nhật hướng dẫn streaming trong `docs/README.md`.

**Interface.** Bổ sung tham số tùy chọn `should_stop: Callable[[], bool] | None = None` vào `consume_events` và `run_producer`, giữ các tham số hiện có. Worker truyền callback từ stop event vào hai vòng lặp. Script start/stop dùng PID cùng thông tin nhận diện tiến trình do chính script tạo.

- [x] Test shutdown có batch chưa đầy, worker có một luồng lỗi và producer bị dừng. Khẳng định consumer flush dữ liệu đã nhận trước khi đóng và lỗi worker tạo exit code khác 0.
- [x] Kiểm tra dừng bằng PID đã hết hạn hoặc PID của tiến trình khác, tránh dừng nhầm tiến trình. Kiểm tra input rate/batch/flush không hợp lệ.
- [x] Hoàn thiện worker và script, đối chiếu thay đổi `close()` đang có với cách thư viện đóng consumer, sửa 8 lỗi Ruff thuộc các file trong phạm vi.
- [x] Chạy test streaming và worker. Khi Docker đã sẵn sàng, chạy producer/consumer có giới hạn sự kiện, kiểm tra batch local/MinIO và thử dừng có kiểm soát.
- [x] Cập nhật trạng thái, commit `fix(streaming): finalize worker lifecycle and shutdown`, push và xác nhận SHA.

**Nghiệm thu.** `.venv/Scripts/python.exe -m pytest tests/test_streaming.py tests/test_stream_worker.py -q`, Ruff các file đã sửa và một lượt kiểm tra Kafka/MinIO thực tế.

## A01. Bàn giao dữ liệu trên MinIO và sửa hướng dẫn vận hành

**Liên quan T03 và bàn giao cho Pháp.**

**File.** Sửa `src/recommender/ingestion/sync_minio.py`, thêm `tests/test_sync_minio.py`. Cập nhật `docs/README.md`, `docs/ARCHITECTURE.md` và đoạn trạng thái lỗi thời trong root `README.md`.

**Interface.** Cung cấp `sync_bronze_to_minio() -> None` dùng settings hiện có, tham số CLI `--download-prefix` và `--output-dir` cho chiều tải về khi cần. Khi công bố batch, data file được upload trước manifest. Bản tải chỉ được đưa vào thư mục đích sau khi checksum đạt.

- [x] Test batch hợp lệ, thiếu manifest, sai checksum, lỗi upload giữa chừng và tải lại cùng dữ liệu. Khẳng định batch chưa đủ file chưa được coi là hoàn tất.
- [x] Hoàn thiện đồng bộ hai chiều cần cho bàn giao, tái sử dụng manifest và cấu trúc object của BronzeSink.
- [ ] Ghi host MinIO dùng chung, cách cấu hình từng máy và quyền đọc/ghi qua biến môi trường. Xác minh máy Pháp truy cập được endpoint và đọc một batch bằng tài khoản của mình.
- [x] Chạy fixture qua sync/download rồi kiểm tra file nhận được giữ nguyên ID và SHA-256. Ghi hướng dẫn chạy và giới hạn cần máy chủ MinIO hoạt động.
- [x] Commit `feat(data): verify and document shared Bronze delivery`, push và xác nhận SHA.

**Nghiệm thu.** Test sync pass, catalog và users 1.000 dòng kiểm tra đúng manifest, một batch events tải được từ kho chung. Khử trùng sự kiện theo ID thuộc bước ETL của Pháp.


**Đã kiểm tra local:** 387 tests pass; tải catalog/users 1.000 dòng và 2 batch 8 events giữ nguyên SHA. Endpoint LAN/tài khoản máy Pháp chưa có, cần nghiệm thu trên máy thứ hai.

## A02. PostgreSQL, kết nối và migration

**Liên quan T08.**

**File.** Tạo `src/recommender/api/db.py`, `src/recommender/api/migrate.py`, `src/recommender/api/migrations/001_initial_schema.sql`, `tests/test_database.py`. Cập nhật package data trong `pyproject.toml` để SQL migration được cài cùng package.

**Interface.** `connect(database_url: str) -> psycopg.Connection` tạo connection UTC. `migrate(database_url: str) -> None` có CLI `python -m recommender.api.migrate` lấy URL từ settings và chạy lại an toàn.

- [x] Viết test DB thật cho bảy bảng, singleton ban đầu, FK kép catalog, rank trùng, NaN/infinity và array antecedent không hợp lệ.
- [x] Viết migration đúng mục 5 của DATA_CONTRACTS, quản lý phiên bản migration và chỉ áp dụng file chưa chạy.
- [x] Kiểm tra chạy migration hai lần, connection timezone và load được file SQL từ package đã cài.
- [x] Chạy `.venv/Scripts/python.exe -m pytest tests/test_database.py -q` với `TEST_DATABASE_URL` trỏ database kiểm thử riêng.
- [x] Commit `feat(db): add versioned PostgreSQL schema and migrations`, push và xác nhận SHA.

**Nghiệm thu.** Database mới tạo được từ migration, constraints có test bằng INSERT thật và singleton tồn tại ngay trước lần import đầu.

## A03. Fixture Gold, kiểm tra snapshot và importer nguyên tử

**Liên quan T08. Phụ thuộc A02.**

**File.** Tạo `src/recommender/pipeline/snapshot.py`, `src/recommender/pipeline/importer.py`, `tests/gold_fixture.py`, `tests/test_importer.py`. Bổ sung PyArrow vào dependency app cho đọc Parquet và nhóm dev/pipeline khi test cần. Tái sử dụng schemas.

**Interface.** `validate_snapshot(path: Path) -> ValidatedSnapshot` trả object chứa manifest đã kiểm tra, fingerprint và đường dẫn bốn bảng. Định nghĩa `ValidatedSnapshot` trong snapshot.py. `import_snapshot(path: Path, database_url: str) -> str` trả `run_id` sau import hoặc retry hợp lệ. `activate_run(run_id: str, database_url: str) -> None` dùng cho rollback tường minh. CLI importer nhận thư mục snapshot local đã tải từ MinIO.

- [x] Tạo helper `make_gold_fixture(output: Path, run_id: str) -> Path` trong test, ghi Parquet và checksum thật, chứa đủ bốn bảng, cả trường hợp association_rules rỗng có schema.
- [x] Test thiếu bảng, sai checksum, đường dẫn file vượt thư mục snapshot, ID/rank trùng, run_id khác manifest, antecedent ngoài catalog và timestamp sau cutoff.
- [x] Viết validator và importer theo thuật toán khóa singleton, kiểm tra fingerprint và phát hành transaction trong ARCHITECTURE mục 5.
- [x] Test lỗi sau khi đã nạp một bảng, cùng run khác fingerprint, retry run ready, hai importer cạnh tranh và rollback cả metadata catalog.
- [x] Chạy test bằng PostgreSQL thật, đọc lại số dòng, con trỏ và fingerprint.
- [x] Commit `feat(serving): validate and atomically import Gold snapshots`, push và xác nhận SHA.

**Nghiệm thu.** `.venv/Scripts/python.exe -m pytest tests/test_importer.py -q` pass. Snapshot cũ vẫn đọc được sau khi lần import kế tiếp thất bại.

## A04. FP-Growth và lọc tương thích mua kèm

**Liên quan phần T06 của Thành.**

**File.** Tạo `src/recommender/ranking/fpgrowth.py`, `src/recommender/ranking/complements.py`, `tests/test_fpgrowth.py`, `tests/test_complements.py`.

**Interface.** `fit_rules(baskets: DataFrame, *, min_support: float, min_confidence: float) -> DataFrame` nhận baskets theo Gold và xuất association_rules một consequent đúng hợp đồng. `recommend_complements(cart_ids: set[str], rules: Sequence[AssociationRule], products: Mapping[str, Product], limit: int) -> list[Recommendation]` là hàm thuần cho API. Cấu hình cặp danh mục và thuộc tính tương thích được truyền tường minh khi cần.

- [x] Tạo fixture giỏ có support/confidence/lift tính tay và test khử trùng sản phẩm trong giỏ, giỏ một item và tập giỏ rỗng.
- [x] Fit PySpark FP-Growth, giữ đúng mẫu số giỏ có từ hai item và metric của luật một consequent. Lưu model cùng cấu hình.
- [x] Test antecedent nhiều item là tập con, một consequent được nhiều luật gợi ý và tie-break theo confidence, lift, ID.
- [x] Test loại item đã ở trong giỏ, thuộc tính tương thích thiếu hoặc sai và cặp ngoài quy tắc.
- [ ] Xuất rules Parquet bằng runner và nối manifest Gold thật của Pháp; Spark fixture đã chạy 5 tests pass.
- [x] Commit `feat(ranking): add FP-Growth complement rules`, push và xác nhận SHA.

**Nghiệm thu.** Rules phù hợp AssociationRule hiện có, có trường hợp mua kèm tính tay khớp kết quả. Bàn giao cho Pháp interface baskets đầu vào và rules đầu ra để nối vào Gold thật.

## A05. FastAPI catalog và ghi sự kiện

**Liên quan T09. Phụ thuộc A02 và A03.**

**File.** Tạo `src/recommender/api/main.py`, `src/recommender/api/errors.py`, `src/recommender/api/routes/__init__.py`, `routes/products.py`, `routes/events.py`, `tests/test_api.py`. Bổ sung HTTPX vào nhóm dev nếu TestClient cần.

**Interface.** `create_app(database_url: str | None = None) -> FastAPI` nhận URL test hoặc lấy từ settings. App có `GET /health`, hai route catalog và `POST /api/v1/events` đúng hợp đồng. Dùng EventCreate và các kiểu response hiện có.

- [x] Test liveness khi DB ngắt, chưa có snapshot, phân trang, ID không có, schema lỗi và envelope lỗi thống nhất.
- [x] Triển khai catalog bằng transaction đọc REPEATABLE READ, thứ tự ID theo collation C.
- [x] Triển khai ghi event với khóa UUID rồi session/cart, so sánh tám trường payload chuẩn hóa và trả sau commit.
- [x] Test hai request cùng UUID, hai UUID tranh cùng session/cart, retry sau đổi snapshot và DB commit lỗi.
- [x] Chạy TestClient với PostgreSQL test và gọi HTTP thật một lượt sau khi bật Uvicorn.
- [x] Commit `feat(api): serve catalog and persist idempotent events`, push và xác nhận SHA.

**Nghiệm thu.** `.venv/Scripts/python.exe -m pytest tests/test_api.py -q` pass. Các mã 200/201/404/409/422/503 được đối chiếu envelope trong hợp đồng.

## A06. Ba endpoint gợi ý từ snapshot

**Liên quan T09. Phụ thuộc A04 và A05.**

**File.** Tạo `src/recommender/api/routes/recommendations.py`, `src/recommender/api/recommendations.py`, `tests/test_recommendation_api.py`. Đăng ký router trong main.py.

**Interface.** Hàm lấy gợi ý nhận connection, `run_id` và kiểu query tương ứng. API xuất RecommendationResponse. Complements tái sử dụng recommend_complements của A04.

- [x] Test user mới nhận popularity, seen bị lọc, candidates được bù đúng thứ tự và rank được đánh lại liên tiếp.
- [x] Test substitutes cùng category, loại sản phẩm gốc, trường hợp thiếu ứng viên và sản phẩm không có.
- [x] Test complements giỏ rỗng, ID trùng, có một ID không có, nhiều antecedent và ràng buộc tương thích.
- [x] Triển khai ba route với cùng active run trong request. Giữ thứ tự MMR từ Gold, tách fallback popularity khỏi thang điểm model.
- [x] Test một request đang đọc khi importer đổi snapshot, khẳng định metadata và kết quả cùng run.
- [x] Commit `feat(api): serve personalized substitute and complement recommendations`, push và xác nhận SHA.

**Nghiệm thu.** `.venv/Scripts/python.exe -m pytest tests/test_recommendation_api.py tests/test_api.py -q` pass. Ba nhóm chạy được bằng fixture Gold.

## A07. Gợi ý thay đổi theo phiên

**Liên quan T10. Phụ thuộc A06.**

**File.** Tạo `src/recommender/ranking/session.py`, `tests/test_session.py`. Cập nhật truy vấn và route personalized trong api/recommendations.py.

**Interface.** `rerank_session(batch_items: Sequence[RecommendationItem], popularity_items: Sequence[RecommendationItem], session_events: Sequence[Event], similarities: Sequence[RecommendationItem], seen_ids: set[str], catalog_ids: set[str], request_time: datetime, limit: int) -> list[Recommendation]`. DB chọn đúng user/session và thứ tự event trước khi gọi hàm.

- [x] Test cutoff 30 phút, giới hạn 20 event, future event và cùng session dưới user khác.
- [x] Test trọng số 1/2/4, half-life 10 phút, chuẩn hóa nguồn riêng, toàn điểm bằng nhau hoặc bằng 0 và fallback khi tín hiệu rỗng.
- [x] Triển khai công thức mục 7.3 của DATA_CONTRACTS, loại seen và không chuẩn hóa lẫn điểm popularity đã bù với ALS.
- [x] Test POST click thành công rồi GET personalized cùng run, xác nhận thứ tự đổi theo fixture. Hai phiên đồng thời giữ lịch sử riêng.
- [x] Đối chiếu giới hạn tối đa 200 ứng viên và rank sau lọc.
- [x] Commit `feat(session): rerank recommendations from recent interactions`, push và xác nhận SHA.

**Nghiệm thu.** `.venv/Scripts/python.exe -m pytest tests/test_session.py tests/test_recommendation_api.py -q` pass, một ca trước/sau click có cùng run_id và thứ tự khác.

## A08. Xuất log demo về Bronze

**Liên quan T10. Phụ thuộc A05.**

**File.** Tạo `src/recommender/pipeline/export_events.py`, `tests/test_export_events.py`. Tái sử dụng `ingestion/bronze.py` và sync MinIO ở A01.

**Interface.** `export_demo_events(database_url: str, output_dir: Path) -> Path` trả đường dẫn manifest sau khi xuất hoàn chỉnh, CLI `python -m recommender.pipeline.export_events`. Quét các event demo trong một snapshot đọc nhất quán.

- [x] Test không có event, timestamp tương lai hoặc quá cũ, UUID bất biến và chỉ tám trường nghiệp vụ trong JSONL.
- [x] Xuất đầy đủ các event demo nhìn thấy trong lượt chạy vào batch mới, ghi file tạm, checksum và manifest cuối cùng.
- [x] Test export hai lần và một transaction commit muộn giữa hai lượt, xác nhận lượt sau có event mới và các dòng lặp giữ nguyên UUID.
- [x] Test lỗi ghi local hoặc upload MinIO giữ được dữ liệu để retry, đối chiếu payload với DB trước khi lập kế hoạch dọn log.
- [x] Commit `feat(pipeline): export committed demo events to Bronze`, push và xác nhận SHA.

**Nghiệm thu.** JSONL được Event.model_validate_json chấp nhận, manifest đúng byte thực tế và pipeline ETL của Pháp có thể dedup theo event_id.

## A09. Website Streamlit đủ ba nhóm gợi ý

**Liên quan T11. Phụ thuộc A06 và A07.**

**File.** Tạo `src/recommender/ui/app.py`, `src/recommender/ui/client.py`, `src/recommender/ui/state.py`, `tests/test_ui.py`. Dùng HTTPX nếu đã thêm, hoặc thư viện HTTP sẵn có đã được kiểm tra.

**Interface.** Client nhận `API_BASE_URL`, timeout và gọi bảy route đã có. State giữ user/session/cart, sản phẩm đang xem và event đang chờ retry. Entry point `streamlit run src/recommender/ui/app.py`.

- [x] Viết test State tạo phiên mới khi đổi user, cart ổn định theo phiên, tạo UUID một lần cho thao tác và giữ UUID khi retry.
- [x] Làm trang danh mục, chi tiết và giỏ với ảnh fallback khi URL ảnh lỗi.
- [x] Gắn Dành cho bạn, Sản phẩm thay thế và Thường thêm cùng vào đúng ngữ cảnh, có so sánh batch với phiên cho demo.
- [x] Ghi view khi mở trang chi tiết, click khi người dùng chọn sản phẩm, add_to_cart khi thêm giỏ. Streamlit rerun do thao tác khác giữ nguyên số event.
- [x] Dùng Streamlit AppTest kiểm tra điều hướng và state, thêm một lượt thao tác trên trình duyệt với API và DB thật.
- [x] Kiểm tra timeout, lỗi API, thiếu snapshot, giỏ rỗng và đổi snapshot giữa các lần lấy metadata. Lưu ảnh demo trong `docs/demo/`.
- [x] Commit `feat(ui): add storefront and three recommendation sections`, push và xác nhận SHA.

**Nghiệm thu.** `.venv/Scripts/python.exe -m pytest tests/test_ui.py -q` pass, UI ghi được event demo và phản ánh gợi ý theo phiên.

## A10. Tích hợp output của Pháp và benchmark phần ứng dụng

**Liên quan phần T12 của Thành. Phụ thuộc A03–A09 và output T04–T07 từ Pháp.**

**File.** Tạo `scripts/run_demo.py`, `scripts/benchmark_api.py`, `tests/test_end_to_end.py`. Chỉ sửa runner dùng chung `scripts/run_pipeline.py` sau khi đối chiếu entry point ETL/model thực tế của Pháp.

**Interface.** Runner nhận `--snapshot-dir` và chạy migrate/import/health theo thứ tự. Benchmark nhận `--base-url`, `--requests`, `--concurrency`, `--output`, ghi p50/p95, tỷ lệ lỗi, cấu hình và run_id. Test fixture có thể chạy sớm, nghiệm thu dữ liệu thật dùng snapshot Pháp bàn giao.

- [ ] Kiểm tra dataset_version, cutoff và schema bốn bảng Gold thật, nối output rules của Thành vào đúng quá trình công bố run của Pháp.
- [ ] Chạy nhập snapshot thật, kiểm tra ba nhóm gợi ý trên cùng user/catalog và fallback.
- [ ] Ghi event từ UI, xuất Bronze, nhờ ETL của Pháp xử lý rồi import batch mới, đối chiếu vòng lặp dữ liệu.
- [x] Test E2E fixture click → DB → session → export.
- [ ] Vòng E2E Gold thật sau khi Pháp bàn giao.
- [x] Đo API personalized/substitutes/complements/events trên fixture sau warm-up, tách thời gian train.
- [x] Benchmark + 400 mẫu thô trong docs/results; ảnh browser trong docs/demo.
- [x] Commit `feat(integration): connect model snapshots and benchmark serving`, push và xác nhận SHA.

**Nghiệm thu.** Vòng E2E thật hoàn tất và có số đo phục vụ, dữ liệu test được tách khỏi log demo dùng để báo cáo.

## A11. Tài liệu, demo và bàn giao phần của Thành

**Liên quan T12 và báo cáo chung. Phụ thuộc A10.**

**File.** Cập nhật `docs/README.md`, `docs/ARCHITECTURE.md`, `docs/DATA_CONTRACTS.md`, `docs/PROJECT_CHECKLIST.md`, `PLAN.md` và bản tóm tắt root README khi thay đổi vận hành cần phản ánh. Thêm `docs/DEMO.md` với kịch bản đã chạy.

- [ ] Ghi các lệnh thật để lấy dữ liệu, migrate, import, chạy API/UI, xuất log và rollback. Chạy trên checkout sạch bằng tài khoản của teammate.
- [ ] Cập nhật ERD theo migration thật và sơ đồ Kafka/MinIO/DB/serving, lưu hình dưới `docs/`.
- [ ] Viết phần báo cáo thuộc Thành về sinh dữ liệu, Kafka/MinIO, PostgreSQL, FP-Growth, API, session, UI và số đo ứng dụng.
- [ ] Đối chiếu bảng kết quả của Pháp với dataset/run đã dùng. Ghi giới hạn dữ liệu mô phỏng và phạm vi đã kiểm chứng.
- [ ] Chuẩn bị demo user cũ/user mới, thay thế, mua kèm, đổi sở thích phiên và batch tiếp theo. Lưu fixture demo để có thể chạy lại.
- [ ] Chạy toàn bộ pytest, Ruff, kiểm tra link tài liệu và hướng dẫn từ đầu đến cuối.
- [ ] Đánh dấu các task đã đạt, ghi commit/SHA push tương ứng, commit `docs(delivery): finalize Thanh implementation and demo guide`, push và xác nhận SHA.

**Nghiệm thu.** Teammate có thể chạy phần ứng dụng bằng tài liệu và output mô hình đã bàn giao. Báo cáo cùng demo sử dụng cùng phiên bản dữ liệu và kết quả thực nghiệm.

## 6. Nhật ký hoàn thành

Chỉ điền commit và SHA remote sau khi task đã đạt kiểm tra.

| Task | Trạng thái | Commit | Kiểm tra và SHA remote |
| --- | --- | --- | --- |
| A00 | Hoàn thành kiểm tra | Xem git log | 383 tests; Kafka→MinIO 8/8; start/stop thật |
| A01 | Code và kiểm tra local xong; chờ máy Pháp | Xem git log | 387 tests; MinIO roundtrip đủ catalog/users/events |
| A02 | Hoàn thành | Xem git log | 9 tests PostgreSQL thật; migration lặp, UTC, constraints |
| A03 | Hoàn thành serving importer | Xem git log | 13 tests; Parquet/checksum/rollback/concurrency |
| A04 | Code và Spark fixture hoàn thành | Xem git log | 5 tests; rules thật chờ Gold publisher |
| A05 | Hoàn thành | Xem git log | 9 tests; HTTP Uvicorn; UUID/identity/commit |
| A06 | Hoàn thành | Xem git log | 13 tests API; giữ run và MMR rank |
| A07 | Hoàn thành | Xem git log | 9 tests; click đổi thứ tự cùng run, tách session |
| A08 | Hoàn thành | Xem git log | 8 tests exporter/sync; replay và late commit |
| A09 | Hoàn thành | Xem git log | 4 tests; browser thật catalog→detail→cart; ảnh docs/demo |
| A10 | Runner/E2E fixture/benchmark xong; chờ Gold thật | Xem git log | 400 HTTP mẫu, 0 lỗi; 2 E2E tests |
| A11 | Chưa thực hiện | | |
