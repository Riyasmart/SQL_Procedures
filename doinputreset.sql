-- DROP PROCEDURE public.inputreset_faretrack_live_do();

CREATE OR REPLACE PROCEDURE public.inputreset_faretrack_live_do()
 LANGUAGE plpgsql
AS $procedure$
declare _sid integer;_eid integer:=0; 
_dtcollected varchar;_currentdate varchar;
begin
RAISE NOTICE 'Procedure Started for input reset';
    -- Check for incomplete extraction
    IF EXISTS (SELECT 1 FROM input WHERE status IN (-1, 0, 4) LIMIT 1) THEN
        RAISE NOTICE 'Extraction not yet complete.';
        RAISE NOTICE 'Please check the input table (status 0 or -1 or 4 found)';
        RAISE NOTICE 'Reset not allowed';
        RAISE NOTICE '------------------';
        RAISE EXCEPTION 'input table status 0 or -1 or 4 is present, reset not allowed';
        RETURN;
    END IF;

_dtcollected=(select to_char(current_date ,'yyyyMMdd'));
-- RAISE NOTICE USING MESSAGE = _dtcollected;
execute 'create table input_'||_dtcollected||' as select * from input';

RAISE NOTICE 'Table backups are done truncation is going on.';

truncate table input RESTART IDENTITY;

RAISE NOTICE 'Procedure completed';
RAISE NOTICE 'Developed by Riyas🙂';


END; 
$procedure$
;

