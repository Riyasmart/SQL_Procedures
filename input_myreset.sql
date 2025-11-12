-- DROP PROCEDURE public.inputreset_faretrack_live_my();

CREATE OR REPLACE PROCEDURE public.inputreset_faretrack_live_my()
 LANGUAGE plpgsql
AS $procedure$
DECLARE 
    _sid INTEGER;
    _eid INTEGER := 0;
    _tc INTEGER;
    _split1 INTEGER := 1;
    _avgcount INTEGER;
    _lookid INTEGER := 1;
    _machine VARCHAR(70);
    _input VARCHAR(1000);
    _db VARCHAR(400);
    _status VARCHAR(5);
    _threadcount INTEGER;
    _testid INTEGER;
    _dtcollected VARCHAR;
    _currentdate VARCHAR;
BEGIN
    RAISE NOTICE '------------------';
    RAISE NOTICE 'Procedure Started';

    -- Check for incomplete extraction
    IF EXISTS (SELECT 1 FROM input_my WHERE status IN (-1, 0) LIMIT 1) THEN
        RAISE NOTICE 'Extraction not yet complete.';
        RAISE NOTICE 'Please check the input_my table (status 0 or -1 found)';
        RAISE NOTICE 'Reset not allowed';
        RAISE NOTICE '------------------';
        RAISE EXCEPTION 'input_my table status 0 or -1 is present, reset not allowed';
        RETURN;
    END IF;

    -- Try to get the latest collection_date + 1 day
    SELECT TO_CHAR(collection_date + INTERVAL '1 day', 'yyyymmdd')
    INTO _dtcollected
    FROM timingdelivery_my
    WHERE collection_date IS NOT NULL
    ORDER BY collection_date DESC
    LIMIT 1;

    -- If no valid collection_date found, use current date instead
    IF _dtcollected IS NULL THEN
        RAISE NOTICE 'No valid collection_date found in timingdelivery_my.';
        RAISE NOTICE 'Using current_date as backup date.';
        _dtcollected := TO_CHAR(current_date, 'yyyymmdd');
    END IF;

    RAISE NOTICE 'Collection date used for backup: %', _dtcollected;

    -- Safely drop backup tables if they exist
    BEGIN
        EXECUTE 'DROP TABLE IF EXISTS input_bak_my';
        EXECUTE 'DROP TABLE IF EXISTS timingdelivery_bak_my';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Warning: could not drop existing backup tables.';
    END;

    -- Create backup tables
    EXECUTE 'CREATE TABLE input_bak_my AS SELECT * FROM input_my';
    EXECUTE 'CREATE TABLE timingdelivery_bak_my AS SELECT * FROM timingdelivery_my';

    RAISE NOTICE 'Backup tables created successfully.';

    -- Create dated backups
    EXECUTE 'CREATE TABLE input_my_' || _dtcollected || ' AS SELECT * FROM input_my';
    EXECUTE 'CREATE TABLE timingdelivery_my_' || _dtcollected || ' AS SELECT * FROM timingdelivery_my';

    RAISE NOTICE 'Dated backup tables created: input_my_% and timingdelivery_my_%', _dtcollected, _dtcollected;

    -- Truncate source tables
    TRUNCATE TABLE input_my RESTART IDENTITY;
    TRUNCATE TABLE timingdelivery_my RESTART IDENTITY;

    RAISE NOTICE 'Tables truncated successfully.';
    RAISE NOTICE 'Procedure completed';
    RAISE NOTICE '------------------';
END;  --- Developed by Riyas
$procedure$
;
