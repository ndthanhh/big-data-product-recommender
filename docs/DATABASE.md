# PostgreSQL phục vụ snapshot và event demo

ERD theo [migration thực tế](../src/recommender/api/migrations/001_initial_schema.sql). Bảy bảng nghiệp vụ cộng bảng kỹ thuật `schema_migrations`. Catalog được phiên bản hóa; product ID có thể xuất hiện trong nhiều run.

```mermaid
erDiagram
    BATCH_RUNS ||--o{ PRODUCTS : run_id
    BATCH_RUNS o|--o| SERVING_STATE : active_run_id
    BATCH_RUNS ||--o{ RECOMMENDATION_ITEMS : run_id
    BATCH_RUNS ||--o{ SEEN_PRODUCTS : run_id
    BATCH_RUNS ||--o{ ASSOCIATION_RULES : run_id
    PRODUCTS ||--o{ EVENTS : "catalog_run_id + product_id"
    PRODUCTS ||--o{ RECOMMENDATION_ITEMS : "run_id + product_id"
    PRODUCTS ||--o{ SEEN_PRODUCTS : "run_id + product_id"
    PRODUCTS ||--o{ ASSOCIATION_RULES : "run_id + consequent_id"
    BATCH_RUNS {
        text run_id PK
        text dataset_version
        char64 manifest_sha256
        text status
        timestamptz training_start
        timestamptz training_end
        bigint seed
        text code_commit
        jsonb parameters
        jsonb environment
        jsonb row_counts
        timestamptz activated_at
    }
    SERVING_STATE {
        smallint singleton_id PK "luon bang 1"
        text active_run_id FK "nullable khi chua import"
        timestamptz updated_at
    }
    PRODUCTS {
        text run_id PK,FK
        text product_id PK
        text name
        text category_id
        text source
        timestamptz collected_at
        text description
        text brand
        text image_url
        text product_url
        jsonb attributes
    }
    EVENTS {
        uuid event_id PK
        text user_id
        text session_id
        text cart_id "nullable"
        text product_id FK
        text catalog_run_id FK
        text event_type
        text source "demo"
        timestamptz event_at
        timestamptz received_at
    }
    RECOMMENDATION_ITEMS {
        text run_id PK,FK
        text kind PK
        text context_id PK
        text product_id PK,FK
        int rank
        float8 score
        text method
    }
    SEEN_PRODUCTS {
        text run_id PK,FK
        text user_id PK
        text product_id PK,FK
    }
    ASSOCIATION_RULES {
        text run_id PK,FK
        text_array antecedent_ids PK
        text consequent_id PK,FK
        float8 support
        float8 confidence
        float8 lift
    }
```

`recommendation_items` còn UNIQUE `(run_id, kind, context_id, rank)`. Mọi score hữu hạn. Antecedent được sắp tăng, không trùng, không null; importer kiểm tra từng ID thuộc catalog. Context substitute phải thuộc cùng catalog/category. `events` tham chiếu catalog của lúc nhận; retry event cũ vẫn hợp lệ sau đổi snapshot.

Importer khóa singleton, nạp bốn bảng và đổi con trỏ cùng transaction. API đọc REPEATABLE READ; event ghi READ COMMITTED với advisory lock UUID rồi session/cart theo thứ tự cố định. API không sửa payload đã tồn tại. Chưa có chính sách xóa snapshot/log tự động; FK giữ catalog đang được tham chiếu.

Raw click số lượng lớn ở Kafka → Bronze MinIO, không nhập toàn bộ vào PostgreSQL. PostgreSQL chứa snapshot phục vụ và event demo tương tác website. [Kiến trúc đầy đủ](ARCHITECTURE.md) · [Chạy demo](DEMO.md).
