# Kế hoạch thực nghiệm và đo hiệu năng

[Trang chủ repo](../README.md) · [Quy ước đồng nhất](CONVENTIONS.md) · [Vận hành](README.md) · [Kiến trúc](ARCHITECTURE.md) · [Hợp đồng v1](DATA_CONTRACTS.md) · [Kế hoạch & Phân công](../PLAN.md) · [Checklist](PROJECT_CHECKLIST.md)

## 1. Câu hỏi và nguyên tắc thực nghiệm

Thực nghiệm trả lời bốn câu hỏi: ALS có cải thiện gợi ý cá nhân hóa so với popularity/TF-IDF không; MMR đổi chất lượng và đa dạng như thế nào; tín hiệu phiên có giúp dự đoán click tiếp theo không; chi phí batch và độ trễ API tăng ra sao khi dữ liệu lớn hơn.

T01 chốt thiết kế, công thức và giao thức đo. T07/T12 thực hiện benchmark và lưu kết quả; tài liệu này không điền số liệu chưa đo. B phụ trách chất lượng và batch, A phụ trách API/UI theo [PLAN.md](../PLAN.md). Dữ liệu mô phỏng cho phép kiểm tra pipeline và các giả định sinh dữ liệu; không được suy rộng trực tiếp thành hiệu quả với người mua thật.

Mọi phương án so sánh dùng cùng `dataset_version`, hai cutoff UTC, catalog có sẵn ở thời điểm dự đoán, relevance, tập user và bộ lọc. Trọng số cố định `view=1`, `click=2`, `add_to_cart=4`. Khóa cấu hình sau validation rồi mới mở kết quả test.

## 2. Dữ liệu và phiên bản

Catalog nhỏ cho kiểm tra tích hợp có nhiều sản phẩm cùng danh mục và cặp có thể mua kèm. Dataset thực nghiệm có ba mức **10.000, 100.000, 1.000.000 event hợp lệ trước bước purge giỏ qua biên**, seed chính `42`. Chạy thêm seed `43`, `44` trên mức 100.000 để quan sát độ nhạy nếu ngân sách cho phép; báo rõ số seed thực chạy. Giữ catalog và quy tắc sinh sở thích/phiên nhất quán giữa các quy mô, ghi số user/session/cart thực tế thay vì chỉ ghi số event.

Mỗi dataset lưu manifest/checksum theo [DATA_CONTRACTS.md](DATA_CONTRACTS.md): phiên bản catalog, nguồn, commit, seed, tham số sinh, thời gian, số lượng theo loại hành vi và nguồn. Các quy mô là dataset độc lập có version riêng; không cắt ngẫu nhiên từ output train đã tổng hợp. Khi dữ liệu thực tế không phủ lịch mẫu bên dưới, tạo một cấu hình split mới trước khi train và lưu cutoff chính xác; không điều chỉnh mốc sau khi thấy điểm test.

## 3. Chia train / validation / test theo thời gian UTC

### 3.1. Hai cutoff và khoảng nửa mở

Lịch chuẩn cho bộ sinh trải từ tháng 10 đến đầu tháng 11/2026:

| Phần | Khoảng theo `event_at` | Sử dụng |
| --- | --- | --- |
| Train | `[2026-10-01T00:00:00Z, 2026-10-25T00:00:00Z)` | Fit các cấu hình ứng viên. |
| Validation | `[2026-10-25T00:00:00Z, 2026-11-01T00:00:00Z)` | Chọn hyperparameter và lambda. |
| Test | `[2026-11-01T00:00:00Z, 2026-11-08T00:00:00Z)` | Đánh giá cuối sau khi khóa cấu hình. |

Hai cutoff là `split_val_time=2026-10-25T00:00:00Z`, `split_test_time=2026-11-01T00:00:00Z`. Start/end giới hạn khoảng quan sát, không phải hai cutoff bổ sung để chọn mô hình. Event đúng cutoff thuộc phần sau; ngoài khoảng quan sát được báo số lượng và không dùng trong phép đo này.

Đánh giá validation dùng model/catalog/hồ sơ/seen chỉ từ train, cutoff 25/10. Sau chọn cấu hình, fit lại từ train + validation đã purge, cutoff 01/11, rồi đánh giá test một lần. Vectorizer, IDF, popularity, ALS factors, luật giỏ, chuẩn hóa đặc trưng và ánh xạ ID đều được tạo lại từ dữ liệu được phép. Test không được dùng để mở vocabulary, cộng weight hoặc chọn seed tốt nhất.

### 3.2. Giỏ đi qua biên

Giỏ là đơn vị nguyên vẹn của bài toán mua kèm. Với mọi `cart_id` khác null, tính `first_event_at`/`last_event_at` từ tất cả event gắn giỏ trong khoảng quan sát. Nếu `first_event_at < cutoff <= last_event_at` ở một trong hai cutoff thì **purge toàn bộ event của giỏ khỏi cả ba phần**. Đây là loại dữ liệu qua biên, không chuyển event tương lai vào train hay tách một giỏ thành hai giỏ.

Giữ danh sách cart bị purge, số event/user/nhãn bị mất và lý do. Dùng cùng bản split đã purge cho mọi mô hình. Event không có cart được chia trực tiếp theo timestamp. Session có thể đi qua cutoff; khi đánh giá phiên, lịch sử chỉ gồm event đã xảy ra trước truy vấn. Việc nhận biết nhóm giỏ qua biên chỉ dùng để loại nhóm, không dùng metadata tương lai làm feature.

Kiểm tra bất biến trước train: không trùng `event_id` giữa các phần; max train < cutoff validation; max phần train+validation < cutoff test; không có cart nằm ở nhiều phần; tập event input của mỗi aggregate/model nằm hoàn toàn trước cutoff tương ứng.

### 3.3. Catalog as-of và item mới

Silver giữ nhiều phiên bản product. Tại cutoff `t`, catalog `C_t` chọn phiên bản mới nhất có `collected_at < t` cho từng product ID. Không dùng mô tả cập nhật sau cutoff. Product đã có trong catalog nhưng chưa từng có tương tác là item cold-start; item chỉ xuất hiện trong catalog sau cutoff không thể được hệ thống biết ở thời điểm dự đoán.

Đánh giá accuracy chính chỉ dùng nhãn trong `C_t`; báo riêng số nhãn bị loại vì sản phẩm chưa có và tỷ lệ nhãn có thể phục vụ. TF-IDF được fit trên `C_t`, kể cả sản phẩm chưa được click; điều này hợp lệ vì metadata đã có trước dự đoán. Các thuật toán khác dùng cùng tập ứng viên `C_t` sau bộ lọc, dù ALS không có factor cho một số item.

## 4. Relevance, tập user và bộ lọc chung

Với user `u`, gọi `H_u` là mọi product đã có event hợp lệ trước cutoff, không phụ thuộc event type. Nhãn `R_u` là tập product có ít nhất một `click` hoặc `add_to_cart` trong cửa sổ đánh giá, nằm trong catalog cutoff và **không thuộc `H_u`**. Khử trùng nhiều event cùng product thành một nhãn nhị phân. `view` không tạo relevance.

Tập đánh giá chính `U` gồm user có `|R_u| > 0`, bao gồm user chưa có lịch sử train. User không còn nhãn sau lọc không được đưa vào mẫu số Recall/NDCG; lưu số lượng user và nhãn bị loại theo từng lý do. Cùng U cho cả bốn phương án, không bỏ user mà ALS không dự đoán được. Dự đoán thiếu phải dùng fallback hoặc nhận danh sách rỗng và điểm accuracy bằng 0.

Mỗi phương án trả tối đa 10 product duy nhất từ `C_t \ H_u`. Lọc trước khi lấy top 10. Không tự đệm bằng item đã xem; thiếu kết quả giữ danh sách ngắn. Tập ứng viên đánh giá là toàn catalog hợp lệ, không dùng negative sampling khác nhau giữa mô hình. Popularity fallback là một phần của phương án vận hành; báo tỷ lệ user/item dùng fallback bên cạnh metric chính.

Báo tách: user có lịch sử / user mới; item có tương tác train / item cold-start trong catalog; source synthetic / demo nếu đủ mẫu. Mỗi phân nhóm ghi số user/nhãn thực tế; nhóm rỗng có trạng thái `not_evaluable`, không gán số 0 giả.

## 5. Metric top 10 và ví dụ đối chiếu

Gọi `L_u=(i_1,...,i_m)`, `m <= 10`, là danh sách có thứ tự sau lọc; `rel(u,r)=1` khi `i_r ∈ R_u`, ngược lại 0. Vị trí thiếu xem như relevance 0.

### 5.1. Recall@10

```text
Recall@10(u) = |set(L_u) ∩ R_u| / |R_u|
Recall@10 = (1 / |U|) × tổng_u Recall@10(u)
```

Đây là macro-average theo user, không phải tổng hit chia tổng nhãn. User có hơn 10 nhãn vẫn dùng toàn bộ `|R_u|` ở mẫu số. `|U|=0` thì metric không xác định: xuất JSON null, trạng thái `not_evaluable`, số mẫu 0.

### 5.2. NDCG@10

```text
DCG@10(u) = tổng_r=1..10 rel(u,r) / log2(r+1)
IDCG@10(u) = tổng_r=1..min(10, |R_u|) 1 / log2(r+1)
NDCG@10(u) = DCG@10(u) / IDCG@10(u)
NDCG@10 = (1 / |U|) × tổng_u NDCG@10(u)
```

Relevance nhị phân nên không cần độ lớn weight trong tử số. Không dùng IDCG theo số item model trả về; model trả ngắn không được giảm mẫu số lý tưởng.

### 5.3. Intra-List Diversity (ILD)

Dùng cùng một bộ vector TF-IDF tham chiếu fit trên catalog cutoff cho tất cả phương án. Với hai vector khác 0, `distance(i,j)=1-cos(v_i,v_j)`. Gọi `P_u` là các cặp vị trí `a < b` trong L có đủ hai vector hợp lệ:

```text
ILD(u) = tổng_(a,b thuộc P_u) [1 - cos(v_ia, v_ib)] / |P_u|
ILD = trung bình ILD(u) trên user có |P_u| > 0
```

Khi mọi item có vector hợp lệ, mẫu số là `m(m-1)/2`, tương đương hệ số `2/[m(m-1)]`. Danh sách ít hơn hai item hoặc không có cặp hợp lệ không có ILD; xuất null và báo số user/cặp dùng thực tế. Không coi vector thiếu là một item rất đa dạng. Báo thêm số vector thiếu/zero và độ dài danh sách trung bình để giải thích chênh lệch.

### 5.4. Catalog Coverage

```text
Coverage@10 = |hợp_u∈U set(L_u)| / |C_t|
```

Mẫu số là toàn catalog có thể phục vụ ở cutoff, gồm cả item cold-start; không chỉ là tập item mà một mô hình đã dự đoán. Dùng cùng U như accuracy cho số liệu chính. Nếu catalog rỗng, xuất null và không chạy benchmark; nếu U rỗng, xuất null để không diễn giải “không có request” thành coverage 0. Coverage vận hành trên tập request demo có thể báo riêng với mẫu số và tập request ghi rõ.

### 5.5. Fixture tính tay cho T07

Với một user, `R={p2,p4}`, `L=[p2,p3,p4]`: Recall@10 = 1; DCG = `1 + 1/log2(4) = 1.5`; IDCG = `1 + 1/log2(3)`; NDCG ≈ **0.919720789**. Nếu catalog có năm item thì Coverage = `3/5 = 0.6`.

Với vector của p2, p3, p4 lần lượt `(1,0)`, `(0,1)`, `(1,0)`, ba khoảng cách là `1, 0, 1`, nên ILD = **2/3**. Fixture khác phải bao gồm danh sách rỗng, một item, không có hit, hơn 10 nhãn và user bị loại vì relevance rỗng. So số thực với tolerance `1e-9`; không dùng assert bằng tuyệt đối cho phép tính log/cosine.

## 6. Các phương án và lựa chọn cấu hình

| Phương án | Dữ liệu/điểm | Fallback và cấu hình |
| --- | --- | --- |
| Popularity | Tổng weight train toàn cục. | Sort giảm điểm rồi tăng product ID. |
| TF-IDF personalized | Hồ sơ user từ vector item train có trọng số, cosine với item. | User không có hồ sơ dùng popularity. |
| Implicit ALS | `implicitPrefs=True`, weight là implicit confidence input. | Drop score không hữu hạn, user/item thiếu factor dùng chính sách fallback chung. |
| ALS + MMR | Cùng ALS đã chọn; MMR trên tối đa 200 ứng viên hợp lệ. | Lambda chọn validation; giữ thứ tự chọn và quy tắc vector thiếu theo hợp đồng. |

Seed ALS `42`, `coldStartStrategy='drop'` đi kèm fallback, không bỏ user khỏi đánh giá. Grid chính: `rank ∈ {16,32}`, `maxIter=10`, `regParam ∈ {0.05,0.1}`, `alpha ∈ {1,10}`: tám cấu hình. Nếu máy không đáp ứng, ghi ngân sách và giảm grid **trước** khi chạy validation; không chỉ báo cấu hình tốt nhất mà bỏ các lần lỗi.

TF-IDF chuẩn hóa lowercase/NFC, ghép name/category/description, CountVectorizer + IDF Spark; `minDF=1`, `vocabSize=20000`, không loại stopword bằng danh sách thay đổi giữa run. MMR thử `lambda ∈ {0.3,0.5,0.7,1.0}` với công thức chuẩn hóa ở [DATA_CONTRACTS.md](DATA_CONTRACTS.md). Lambda 1 là kiểm tra thứ tự phù hợp không có phạt trùng nội dung.

Chọn theo NDCG@10 validation cao nhất; nếu chênh dưới `1e-6`, ưu tiên Recall@10, rồi thời gian train thấp hơn, rồi thứ tự cấu hình đã liệt kê. Báo ILD và coverage của mọi cấu hình MMR, không chọn theo test hoặc mặc định ghi MMR “tốt hơn”. Sau khi chọn, fit lại đúng cấu hình trên train+validation và đo test. Không tune weight hành vi trong thí nghiệm chính.

FP-Growth thử `minSupport ∈ {0.001,0.01}`, `minConfidence ∈ {0.2,0.5}` trên train; chọn theo phục hồi item giỏ validation, rồi số luật/chi phí nếu bằng điểm. Ngưỡng và số giỏ thực dùng phải được ghi vì dữ liệu nhỏ có thể không có luật nào.

## 7. Thay thế, mua kèm và tương thích

Substitutes có kiểm tra bắt buộc: cùng category, không chứa product gốc, không trùng, score hữu hạn, tie-break xác định và không dùng metadata tương lai. Khi chưa có nhãn tương đương do người đánh giá gán, không gọi cosine là độ chính xác thay thế. Chuẩn bị ca review thủ công theo danh mục và ghi số ca đạt/tổng ca cùng tiêu chí.

Đánh giá complements dùng giỏ validation/test không nằm trong dữ liệu fit, có ít nhất hai item trong catalog cutoff. Chọn một item holdout bằng thứ tự hash SHA-256 của `(seed, cart_id, product_id)` tăng dần; phần còn lại là giỏ query. Không đưa item holdout hoặc sự kiện của giỏ đó vào train. Dùng cùng rule matching và bộ lọc tương thích như API, tính Hit/Recall@10 của item bị giữ lại và tỷ lệ giỏ có ít nhất một gợi ý. Không có luật thì prediction rỗng, điểm 0 cho giỏ đủ điều kiện.

Lưu confidence, support, lift của luật thắng cho từng prediction, số giỏ không có luật và số cặp bị loại bởi tương thích/thiếu thuộc tính. Dùng các fixture connector trùng, khác và thiếu để kiểm tra quy tắc; chỉ tuyên bố tương thích trong phạm vi quy tắc có dữ liệu, không xem đồng xuất hiện là bằng chứng tương thích kỹ thuật.

## 8. Đánh giá tín hiệu phiên bằng click tiếp theo

So sánh ba cách trên **cùng batch cố định**: batch thuần; chỉ điểm phiên; kết hợp `0.7 batch + 0.3 session`. Hai hằng số cửa sổ 30 phút/tối đa 20 event và half-life 10 phút giữ nguyên theo hợp đồng v1. Chỉ điểm phiên không có tín hiệu thì dùng popularity và báo tỷ lệ fallback.

Replay từng session theo `(event_at, event_id)` tăng. Trước mỗi click mục tiêu tại thời điểm t, tính gợi ý chỉ từ event trước nó, không thêm click mục tiêu vào hồ sơ. Với offline Silver không có thời điểm nhận server, giả định event khả dụng theo `event_at` và công bố giới hạn này; phép đo demo có `received_at` phải dùng chỉ event đã commit trước request. Không trộn giả định offline với số đo xử lý late event thực tế.

Tập item hợp lệ chung loại seen snapshot và các item trong cửa sổ phiên hiện tại; nhãn không còn đủ điều kiện bị loại và thống kê. Áp cùng bộ lọc này cho cả ba nhánh trong benchmark để so điểm công bằng; nhánh batch ở đây có thêm bộ lọc phiên của benchmark so với endpoint batch thuần trong demo. Báo Hit@10/NDCG@10 cho nhãn một click, số query, số session, tỷ lệ fallback và phân nhóm session đổi danh mục. Kịch bản demo vận hành riêng gọi endpoint batch không session và endpoint có session, xác nhận cùng `run_id` trước khi so danh sách.

## 9. Đo batch ở 10k / 100k / 1M events

Ghi máy đo: CPU/model/số core, RAM, ổ lưu trữ, OS, Python/Java/Spark/PostgreSQL, commit, dataset version, checksum, seed, `spark.master`, memory, shuffle partitions, số partition input và tổng byte input/output. Mốc local ban đầu là `local[2]`, `spark.sql.session.timeZone=UTC`, `spark.sql.shuffle.partitions=4`; memory được chọn phù hợp máy trước khi đo và giữ cố định giữa ba quy mô. Không điền cấu hình RAM của máy chưa kiểm tra.

| Giai đoạn | Bắt đầu → kết thúc | Đơn vị |
| --- | --- | --- |
| `spark_startup` | Trước khởi tạo Spark → session sẵn sàng. | Giây, tách khỏi ETL. |
| `etl` | Đọc Bronze → Silver đã ghi xong, đọc kiểm tra và manifest hoàn tất. | Giây, events/giây, input/accepted/rejected. |
| `aggregate` | Đọc split → interactions và baskets được materialize. | Giây. |
| `fit_popularity`, `fit_tfidf`, `fit_als`, `fit_fpgrowth` | Trước fit → model/đặc trưng lưu xong. | Giây theo từng thuật toán. |
| `recommend_export` | Sinh candidates → Gold hoàn chỉnh và checksum. | Giây, số dòng, byte output. |
| `import_activate` | Bắt đầu import DB → commit đổi active run. | Giây, số dòng mỗi bảng. |

Dùng đồng hồ monotonic (`perf_counter`) cho elapsed time. Spark lazy evaluation yêu cầu action/ghi dữ liệu hoàn tất trong phạm vi đo, không chỉ đo thời gian tạo DataFrame. Một lượt warm-up trên dữ liệu nhỏ không tính vào kết quả; sau đó ba lượt đo độc lập cho mỗi quy mô/cấu hình. Dùng thư mục output/run riêng, không đọc nhầm cache của lượt trước. Báo cache policy; phép đo chính đọc lại từ disk, không gọi đó là cold OS cache nếu không kiểm soát cache hệ điều hành.

Báo median, min, max của ba lượt, input throughput và output size; peak process RSS nếu công cụ đo có sẵn, ghi phạm vi có bao gồm JVM hay không. Nếu OOM/timeout ở 1M, lưu lỗi và mức lớn nhất đã chạy, không nội suy thành một lượt thành công. Đo tách chế độ chỉ batch và batch chạy cùng demo để thấy tranh chấp tài nguyên.

## 10. Đo độ trễ API và phản hồi UI

### 10.1. Kịch bản API

Chạy trên loopback, giữ HTTP keep-alive, cùng dataset/run/DB, ghi số worker, pool size, timeout và logging. Mỗi endpoint/kịch bản warm-up 100 request, sau đó đo 1.000 request ở mỗi mức concurrency **1, 5, 10**, lặp ba lượt. Kịch bản ghi event dùng UUID mới từng request; kịch bản retry dùng cùng payload/UUID và báo riêng, không trộn với insert mới.

| Endpoint/kịch bản | Kiểm soát |
| --- | --- |
| Personalized batch | User có lịch sử và user mới đo riêng; limit 10. |
| Personalized session | User/session có 20 event trong cửa sổ, cùng active run như batch. |
| Substitutes | Product có ít nhất 10 ứng viên và product ít ứng viên đo riêng. |
| Complements | Giỏ 2–5 item, khớp luật; giỏ không luật/giỏ rỗng là ca riêng. |
| POST event mới | Product hợp lệ, source demo, UUID duy nhất; chỉ thành công khi 201 sau commit. |
| POST retry | Payload giữ nguyên, kỳ vọng 200; đo riêng 409 conflict trong kiểm tra chức năng. |

Thời gian client từ trước gửi đến đọc hết response body. Với n latency thành công sắp tăng, dùng nearest-rank: `p50=x[ceil(0.50n)-1]`, `p95=x[ceil(0.95n)-1]` theo index bắt đầu 0. Lưu raw latency để tính lại. Báo p50/p95 theo từng lượt và tổng hợp từ raw mẫu cùng kịch bản; không lấy trung bình p95 rồi gọi là p95 chung.

Báo đồng thời số request gửi/thành công/lỗi, status code, timeout, tỷ lệ lỗi, elapsed wall time và throughput. Không bỏ request lỗi khỏi báo cáo; latency thành công và latency lỗi tách riêng. Không dùng `/health` thay cho phép đo đọc DB. Khi chạy cùng batch, dùng cùng protocol và ghi thời điểm chồng lấp.

### 10.2. Từ thao tác UI đến danh sách mới

Chuẩn bị user có lịch sử danh mục A, giữ nguyên snapshot, chuyển click sang danh mục B. Mỗi lượt ghi thời điểm thao tác, event UUID, POST commit acknowledgment, GET kết quả và lúc UI hiển thị xong. Bỏ lượt warm-up đầu, đo 30 thao tác hợp lệ; ghi top 10 trước/sau, run ID và session ID.

Báo p50/p95 end-to-end, thời gian POST và GET riêng, tỷ lệ lỗi và tỷ lệ thao tác tạo đúng một event logic. Đổi thứ tự là điều kiện quan sát của fixture có tín hiệu rõ, không phải cam kết mọi click làm tất cả top 10 đổi. Tách hai phiên đồng thời để kiểm tra không lẫn lịch sử. Mục tiêu độ trễ vận hành được P11-07 chốt từ lượt đo ban đầu; T01 không tuyên bố đã đạt một ngưỡng chưa đo.

## 11. Lưu kết quả, kiểm tra và trình bày

Lưu dưới `data/gold/<run_id>/evaluation/` theo các file được quy định sau; đây là hợp đồng output của benchmark, chưa phải artifact đã có:

| File | Nội dung bắt buộc |
| --- | --- |
| `config.json` | Dataset/hash, cutoff, commit/dirty, seed, cấu hình model/Spark, candidate/filter policy và môi trường. |
| `split_summary.json` | Counts mỗi phần/source/type; cart purge; user/nhãn bị loại; số item cold-start/future. |
| `quality.csv` | Một dòng mỗi model/split/cohort: Recall@10, NDCG@10, ILD, coverage, user/label/pair counts, average list length, fallback rate. |
| `batch_timings.csv` | Quy mô, repetition, stage, seconds, rows, bytes, memory nếu đo được, status/error. |
| `api_latencies.csv` | Repetition, endpoint, scenario, concurrency, request ID, elapsed_ms, status/error và run ID. |
| `api_summary.json` | Warm-up, sample counts, phương pháp percentile, p50/p95, throughput và tỷ lệ lỗi từng kịch bản. |
| `session_replay.csv` | Query/user/session/time, target, rank hit, phương án, run ID và lịch sử được phép. |
| `ui_feedback.csv` | Event UUID, session/run ID, mốc thao tác/commit/hiển thị và lỗi nếu có. |

Null dùng cho metric không xác định; CSV dùng ô rỗng kèm `status=not_evaluable`, không dùng chuỗi đánh dấu nội dung bỏ dở hoặc số bịa. Manifest run liệt kê các file và checksum thực. Biểu đồ xuất từ các file này, ghi đơn vị/trục, quy mô, số lượt và môi trường. Báo cả tăng/giảm so với baseline; nếu baseline bằng 0 thì chỉ ghi chênh lệch tuyệt đối, không chia để tính phần trăm.

Nghiệm thu trước báo cáo: metric khớp fixture tính tay; split không rò rỉ; cùng input/seed tái tạo cùng dữ liệu chuẩn hóa và split; model tải lại dùng đúng mapping; mọi model dùng cùng tập user/candidate policy; bảng/biểu đồ truy được về run/dataset/code. Kết quả số thực mô hình so theo tolerance được ghi, không hứa byte-identical cho train song song. Cập nhật các mục P6/P12/P13 trong [PROJECT_CHECKLIST.md](PROJECT_CHECKLIST.md) chỉ sau khi có bằng chứng chạy.
