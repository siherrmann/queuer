CREATE OR REPLACE FUNCTION notify_event()
RETURNS TRIGGER AS $$
DECLARE
    data JSON;
    channel TEXT;
BEGIN
    IF (TG_TABLE_NAME = 'job') OR (TG_TABLE_NAME = 'worker') THEN
        channel := TG_TABLE_NAME;
    ELSE
        channel := 'job_archive';
    END IF;

    IF (TG_OP = 'DELETE') THEN
        data = row_to_json(OLD);
    ELSE
        data = row_to_json(NEW);
    END IF;
    PERFORM pg_notify(channel, data::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;