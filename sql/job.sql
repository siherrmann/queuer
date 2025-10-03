CREATE OR REPLACE FUNCTION update_job_initial(input_worker_id BIGINT)
RETURNS TABLE (
    id BIGINT,
    rid UUID,
    worker_id BIGINT,
    worker_rid UUID,
    options JSONB,
    task_name VARCHAR(100),
    parameters JSONB,
    status VARCHAR(50),
    scheduled_at TIMESTAMP,
    started_at TIMESTAMP,
    schedule_count INT,
    attempts INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    WITH current_concurrency AS (
        SELECT COUNT(*) AS count
        FROM job
        WHERE job.worker_id = input_worker_id
        AND job.status = 'RUNNING'
    ),
    current_worker AS (
        SELECT
            worker.id,
            worker.rid,
            worker.available_tasks,
            worker.available_next_interval,
            worker.max_concurrency,
            COALESCE(cc.count, 0) AS current_concurrency
        FROM worker, current_concurrency AS cc
        WHERE worker.id = input_worker_id
        AND (worker.max_concurrency > COALESCE(cc.count, 0))
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
                    job.options->'schedule'->>'next_interval' IS NULL
                    OR job.options->'schedule'->>'next_interval' = ''
                    OR job.options->'schedule'->>'next_interval' = ANY(cw.available_next_interval::VARCHAR[])
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
        schedule_count = job.schedule_count + 1,
        attempts = job.attempts + 1,
        updated_at = CURRENT_TIMESTAMP
    FROM current_worker AS cw, job_ids
    WHERE job.id = ANY(SELECT job_ids.id FROM job_ids)
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
    input_id BIGINT,
    input_status VARCHAR(50),
    input_results JSONB,
    input_error TEXT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_status VARCHAR(50),
    output_scheduled_at TIMESTAMP,
    output_started_at TIMESTAMP,
    output_schedule_count INT,
    output_attempts INT,
    output_results JSONB,
    output_error TEXT,
    output_created_at TIMESTAMP,
    output_updated_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    WITH jobs_old AS (
        DELETE FROM job
        WHERE id = input_id
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
        input_status,
        jobs_old.scheduled_at,
        jobs_old.started_at,
        jobs_old.schedule_count,
        jobs_old.attempts,
        input_results,
        input_error,
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

CREATE OR REPLACE FUNCTION update_job_final_encrypted(
    input_id BIGINT,
    input_status VARCHAR(50),
    input_results TEXT,
    input_error TEXT,
    input_encryption_key TEXT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_worker_id BIGINT,
    output_worker_rid UUID,
    output_options JSONB,
    output_task_name VARCHAR(100),
    output_parameters JSONB,
    output_status VARCHAR(50),
    output_scheduled_at TIMESTAMP,
    output_started_at TIMESTAMP,
    output_schedule_count INT,
    output_attempts INT,
    output_results JSONB,
    output_error TEXT,
    output_created_at TIMESTAMP,
    output_updated_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    WITH jobs_old AS (
        DELETE FROM job
        WHERE id = input_id
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
        results_encrypted,
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
        input_status,
        jobs_old.scheduled_at,
        jobs_old.started_at,
        jobs_old.schedule_count,
        jobs_old.attempts,
        pgp_sym_encrypt(input_results, input_encryption_key),
        input_error,
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
        input_results::JSONB,
        error,
        created_at,
        updated_at;
END;
$$ LANGUAGE plpgsql;