CREATE DOMAIN business_id AS TEXT COLLATE "C"
    CHECK (length(VALUE) BETWEEN 1 AND 128 AND VALUE = btrim(VALUE)
           AND VALUE !~ '[[:cntrl:]]');
CREATE DOMAIN path_id AS TEXT COLLATE "C"
    CHECK (VALUE ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$');
CREATE FUNCTION valid_string_map(value JSONB) RETURNS BOOLEAN
    LANGUAGE SQL IMMUTABLE STRICT AS $$
    SELECT CASE WHEN jsonb_typeof(value) <> 'object' THEN FALSE ELSE
        NOT EXISTS (SELECT 1 FROM jsonb_each(value) AS e WHERE e.key='' OR jsonb_typeof(e.value)<>'string') END
    $$;
CREATE FUNCTION valid_antecedent(value TEXT[]) RETURNS BOOLEAN
    LANGUAGE SQL IMMUTABLE STRICT AS $$
    SELECT array_ndims(value)=1 AND cardinality(value)>0 AND array_position(value,NULL) IS NULL
       AND value=(SELECT array_agg(DISTINCT x COLLATE "C" ORDER BY x COLLATE "C") FROM unnest(value) AS x)
    $$;
CREATE TABLE batch_runs (
    run_id path_id PRIMARY KEY,
    dataset_version path_id NOT NULL,
    schema_version SMALLINT NOT NULL CHECK(schema_version=1),
    manifest_sha256 CHAR(64) NOT NULL CHECK(manifest_sha256 ~ '^[a-f0-9]{64}$'),
    status TEXT NOT NULL CHECK(status IN ('loading','ready','failed')),
    training_start TIMESTAMPTZ NOT NULL,
    training_end TIMESTAMPTZ NOT NULL CHECK(training_start<training_end),
    seed BIGINT NOT NULL,
    code_commit TEXT NOT NULL,
    parameters JSONB NOT NULL CHECK(jsonb_typeof(parameters)='object'),
    environment JSONB NOT NULL CHECK(jsonb_typeof(environment)='object'),
    row_counts JSONB NOT NULL CHECK(jsonb_typeof(row_counts)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at TIMESTAMPTZ,
    error_message TEXT
);
CREATE TABLE serving_state (
    singleton_id SMALLINT PRIMARY KEY CHECK(singleton_id=1),
    active_run_id path_id REFERENCES batch_runs(run_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO serving_state(singleton_id) VALUES(1);
CREATE TABLE products (
    run_id path_id NOT NULL REFERENCES batch_runs(run_id),
    product_id business_id NOT NULL CHECK(product_id ~ '^[^[:space:]:]+:[^[:space:]]+$'),
    name TEXT NOT NULL CHECK(length(name) BETWEEN 1 AND 512 AND btrim(name)<>''),
    category_id business_id NOT NULL CHECK(category_id=lower(category_id)),
    source business_id NOT NULL CHECK(length(source)<=64 AND source=split_part(product_id,':',1)),
    collected_at TIMESTAMPTZ NOT NULL,
    description TEXT CHECK(length(description)<=20000),
    brand TEXT CHECK(length(brand)<=256),
    image_url TEXT CHECK(length(image_url)<=2048 AND image_url ~ '^https?://[^[:space:]]+$'),
    product_url TEXT CHECK(length(product_url)<=2048 AND product_url ~ '^https?://[^[:space:]]+$'),
    attributes JSONB NOT NULL DEFAULT '{}'::JSONB CHECK(valid_string_map(attributes)),
    PRIMARY KEY(run_id,product_id)
);
CREATE INDEX products_category ON products(run_id,category_id,product_id);
CREATE TABLE events (
    event_id UUID PRIMARY KEY,
    user_id business_id NOT NULL,
    session_id business_id NOT NULL,
    product_id business_id NOT NULL,
    event_type TEXT NOT NULL CHECK(event_type IN ('view','click','add_to_cart')),
    event_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK(source='demo'),
    cart_id business_id CHECK(event_type<>'add_to_cart' OR cart_id IS NOT NULL),
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    catalog_run_id path_id NOT NULL,
    FOREIGN KEY(catalog_run_id,product_id) REFERENCES products(run_id,product_id)
);
CREATE INDEX events_session ON events(user_id,session_id,event_at DESC,received_at DESC,event_id);
CREATE INDEX events_received ON events(received_at,event_id);
CREATE INDEX events_cart_owner ON events(cart_id,user_id,session_id);
CREATE INDEX events_session_owner ON events(session_id,user_id);
CREATE TABLE recommendation_items (
    run_id path_id NOT NULL REFERENCES batch_runs(run_id),
    kind TEXT NOT NULL CHECK(kind IN ('personalized','substitute','popular')),
    context_id business_id NOT NULL,
    product_id business_id NOT NULL,
    rank INTEGER NOT NULL CHECK(rank>0),
    score DOUBLE PRECISION NOT NULL CHECK(score>'-Infinity'::float8 AND score<'Infinity'::float8),
    method TEXT NOT NULL CHECK(method IN ('popularity','tfidf','als','als_mmr')),
    CHECK(kind<>'popular' OR (context_id='global' AND method='popularity' AND score>0)),
    CHECK(kind<>'substitute' OR (context_id<>product_id AND method='tfidf' AND score BETWEEN 0 AND 1)),
    PRIMARY KEY(run_id,kind,context_id,product_id),
    UNIQUE(run_id,kind,context_id,rank),
    FOREIGN KEY(run_id,product_id) REFERENCES products(run_id,product_id)
);
CREATE TABLE seen_products (
    run_id path_id NOT NULL REFERENCES batch_runs(run_id),
    user_id business_id NOT NULL,
    product_id business_id NOT NULL,
    PRIMARY KEY(run_id,user_id,product_id),
    FOREIGN KEY(run_id,product_id) REFERENCES products(run_id,product_id)
);
CREATE TABLE association_rules (
    run_id path_id NOT NULL REFERENCES batch_runs(run_id),
    antecedent_ids TEXT[] COLLATE "C" NOT NULL CHECK(valid_antecedent(antecedent_ids)),
    consequent_id business_id NOT NULL CHECK(NOT consequent_id=ANY(antecedent_ids)),
    support DOUBLE PRECISION NOT NULL CHECK(support>0 AND support<=1),
    confidence DOUBLE PRECISION NOT NULL CHECK(confidence>0 AND confidence<=1),
    lift DOUBLE PRECISION NOT NULL CHECK(lift>0 AND lift<'Infinity'::float8),
    PRIMARY KEY(run_id,antecedent_ids,consequent_id),
    FOREIGN KEY(run_id,consequent_id) REFERENCES products(run_id,product_id)
);
CREATE INDEX rules_antecedent ON association_rules USING GIN(antecedent_ids);
CREATE INDEX rules_rank ON association_rules(run_id,confidence DESC,lift DESC,consequent_id);
