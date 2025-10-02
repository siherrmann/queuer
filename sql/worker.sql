CREATE OR REPLACE FUNCTION insert_worker(
    p_name TEXT,
    p_options JSONB,
    p_max_concurrency INTEGER
)
RETURNS TABLE (
    id BIGINT,
    rid UUID,
    name TEXT,
    options JSONB,
    max_concurrency INTEGER,
    status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
)
AS $$
BEGIN
    RETURN QUERY
    INSERT INTO worker (name, options, max_concurrency)
    VALUES (p_name, p_options, p_max_concurrency)
    RETURNING
        id, rid, name, options, max_concurrency, status, created_at, updated_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_worker(
    p_name TEXT,
    p_options JSONB,
    p_available_tasks INTEGER,
    p_available_next_interval INTEGER,
    p_max_concurrency INTEGER,
    p_status TEXT,
    p_rid UUID
)
RETURNS TABLE (
    id BIGINT,
    rid UUID,
    name TEXT,
    options JSONB,
    available_tasks INTEGER,
    available_next_interval INTEGER,
    max_concurrency INTEGER,
    status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
)
AS $$
BEGIN
    RETURN QUERY
    UPDATE worker
    SET
        name = p_name,
        options = p_options,
        available_tasks = p_available_tasks,
        available_next_interval = p_available_next_interval,
        max_concurrency = p_max_concurrency,
        status = p_status,
        updated_at = CURRENT_TIMESTAMP
    WHERE
        rid = p_rid
    RETURNING
        id,
        rid,
        name,
        options,
        available_tasks,
        available_next_interval,
        max_concurrency,
        status,
        created_at,
        updated_at;
END;
$$ LANGUAGE plpgsql;