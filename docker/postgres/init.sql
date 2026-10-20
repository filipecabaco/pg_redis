CREATE EXTENSION IF NOT EXISTS pg_redis;

-- With redis.aclfile set the server will not start without the file, as
-- Redis will not. An empty one is a valid ACL file: the default user, with
-- redis.password.
DO $$
BEGIN
    IF current_setting('redis.aclfile', true) <> '' THEN
        EXECUTE format(
            'COPY (SELECT 1 WHERE false) TO %L',
            current_setting('data_directory') || '/' || current_setting('redis.aclfile')
        );
    END IF;
END
$$;
