CREATE TABLE mewbase_binder(
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(500)
);

CREATE UNIQUE INDEX mewbase_binder_unique_name ON mewbase_binder(name);

CREATE TABLE mewbase_binder_data(
    id BIGSERIAL PRIMARY KEY,
    binder_id BIGINT REFERENCES mewbase_binder(id) ON DELETE RESTRICT,
    key TEXT,
    data bytea
);

CREATE UNIQUE INDEX mewbase_binder_data_unique_binder_key ON mewbase_binder_data(binder_id, key);
