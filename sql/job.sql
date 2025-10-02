CREATE OR REPLACE FUNCTION update_job_initial(p_worker_id bigint)
RETURNS TABLE (
    id bigint,
    rid uuid,
    worker_id bigint,
    worker_rid uuid,
    options jsonb,
    task_name text,
    parameters jsonb,
    status text,
    scheduled_at timestamptz,
    started_at timestamptz,
    schedule_count integer,
    attempts integer,
    created_at timestamptz,
    updated_at timestamptz
) AS $$
BEGIN
    RETURN QUERY
    WITH current_concurrency AS (
        SELECT COUNT(*) AS count
        FROM job
        WHERE worker_id = p_worker_id
        AND status = 'RUNNING'
    ),
    current_worker AS (
        SELECT
            id,
            rid,
            available_tasks,
            available_next_interval,
            max_concurrency,
            COALESCE(cc.count, 0) AS current_concurrency
        FROM worker, current_concurrency AS cc
        WHERE id = p_worker_id
        AND (max_concurrency > COALESCE(cc.count, 0))
        FOR UPDATE
    ),
    job_ids AS (
        SELECT j.id
        FROM current_worker AS cw, current_concurrency AS cc,
        LATERAL (
            SELECT job.id
            FROM job
            WHERE
                job.task_name = ANY(cw.available_tasks::VARCHAR[])
                AND (
                    options->'schedule'->>'next_interval' IS NULL
                    OR options->'schedule'->>'next_interval' = ''
                    OR options->'schedule'->>'next_interval' = ANY(cw.available_next_interval::VARCHAR[])
                )
                AND (
                    job.status = 'QUEUED'
                    OR (job.status = 'SCHEDULED' AND job.scheduled_at <= (CURRENT_TIMESTAMP + INTERVAL '10 minutes'))
                )
            ORDER BY job.created_at ASC
            LIMIT (cw.max_concurrency - COALESCE(cc.count, 0))
            FOR UPDATE SKIP LOCKED
        ) AS j
    )
    UPDATE job SET
        worker_id = cw.id,
        worker_rid = cw.rid,
        status = 'RUNNING',
        started_at = CURRENT_TIMESTAMP,
        schedule_count = schedule_count + 1,
        attempts = attempts + 1,
        updated_at = CURRENT_TIMESTAMP
    FROM current_worker AS cw, job_ids
    WHERE job.id = ANY(SELECT id FROM job_ids)
    AND EXISTS (SELECT 1 FROM current_worker)
    RETURNING
        job.id,
        job.rid,
        job.worker_id,
        job.worker_rid,
        job.options,
        job.task_name,
        job.parameters,
        job.status,
        job.scheduled_at,
        job.started_at,
        job.schedule_count,
        job.attempts,
        job.created_at,
        job.updated_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_job_final(
    p_id bigint,
    p_status text,
    p_results jsonb,
    p_error text
)
RETURNS TABLE (
    id bigint,
    rid uuid,
    worker_id bigint,
    worker_rid uuid,
    options jsonb,
    task_name text,
    parameters jsonb,
    status text,
    scheduled_at timestamptz,
    started_at timestamptz,
    schedule_count integer,
    attempts integer,
    results jsonb,
    error text,
    created_at timestamptz,
    updated_at timestamptz
) AS $$
BEGIN
    RETURN QUERY
    WITH jobs_old AS (
        DELETE FROM job
        WHERE id = p_id
        RETURNING
            id,
            rid,
            worker_id,
            worker_rid,
            options,
            task_name,
            parameters,
            scheduled_at,
            started_at,
            schedule_count,
            attempts,
            created_at,
            updated_at
    )
    INSERT INTO job_archive (
        id,
        rid,
        worker_id,
        worker_rid,
        options,
        task_name,
        parameters,
        status,
        scheduled_at,
        started_at,
        schedule_count,
        attempts,
        results,
        error,
        created_at,
        updated_at
    )
    SELECT
        jobs_old.id,
        jobs_old.rid,
        jobs_old.worker_id,
        jobs_old.worker_rid,
        jobs_old.options,
        jobs_old.task_name,
        jobs_old.parameters,
        p_status,
        jobs_old.scheduled_at,
        jobs_old.started_at,
        jobs_old.schedule_count,
        jobs_old.attempts,
        p_results,
        p_error,
        jobs_old.created_at,
        CURRENT_TIMESTAMP
    FROM jobs_old
    RETURNING
        id,
        rid,
        worker_id,
        worker_rid,
        options,
        task_name,
        parameters,
        status,
        scheduled_at,
        started_at,
        schedule_count,
        attempts,
        results,
        error,
        created_at,
        updated_at;
END;
$$ LANGUAGE plpgsql;