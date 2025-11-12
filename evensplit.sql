-- DROP PROCEDURE public.split_launch(int4, int4);

CREATE OR REPLACE PROCEDURE public.split_launch(IN _pid integer, IN _webcode integer)
 LANGUAGE plpgsql
AS $procedure$

declare _sid integer;_eid integer:=0;_tc integer;_split1 integer:=1;_avgcount integer;_lookid integer:=1;_machine varchar(70);_input varchar(1000);_db varchar(400);_status varchar(5);_threadcount integer;_testid integer; _split integer;

begin
    RAISE NOTICE 'Procedure Started';
create temp table if not exists sp_input(c_input varchar(50));
create temp table if not exists sp_status(c_status integer);
create temp table if not exists sp_runids(id serial,c_id integer);
create temp table if not exists sp_machines(id serial,machineip varchar(50));

    truncate table sp_input RESTART IDENTITY;
    insert into sp_input(c_input)
    select input from pyscripts where source_id=_webcode and processid=_pid limit 1;
    select c_input into _input from sp_input; 
    RAISE NOTICE 'input: %', _input;
    truncate table sp_status RESTART IDENTITY;
    insert into sp_status(c_status)
    select status from pyscripts where source_id=_webcode and processid=_pid limit 1;
    select c_status into _status from sp_status; 

    RAISE NOTICE 'input: %', _input;

    truncate table sp_runids RESTART IDENTITY;
    
    execute 'insert into sp_runids(c_id) select id from '||_input||' WHERE source_id='||_webcode||' and status = 0 order by 1';
    
    _split=(select count(1) from  machines where source_id=_webcode);
    truncate table sp_machines RESTART IDENTITY;
    insert into sp_machines(machineip)
    select machineip from machines where source_id=_webcode limit _split;

select count(1) into _tc from sp_runids;
select _tc/_split into _avgcount;

delete FROM pyscripts WHERE id IN 
(SELECT id FROM 
  (SELECT id, ROW_NUMBER() OVER 
    (partition BY processid,source_id ORDER BY id) AS rnum 
  FROM pyscripts where processid=_pid and source_id=_webcode) t
WHERE t.rnum > 1);

while _split1<=_split loop  
--mip:='';  
        select _eid+1 into _sid;
        select c_id into _eid from sp_runids where id = _lookid+_avgcount-1;
        select machineip into _machine from sp_machines where id=_split1;

    if _split1=1 then 
            update pyscripts set startid=_sid,endid=_eid,machineip=_machine,totalcount=_avgcount where processid=_pid and source_id=_webcode;
    end if;
    
    if _split1<_split and _split!=1 then 
       insert into pyscripts(source_id,script,threadcount,machines,createddate,dtmodified,developerid,changecount,runstatus,processid,startid,endid,modifierid,modifiedfor,lastrun,machineip,input,output,db,status,proxyid,"Server" ,s3offline,msdb,crawltype,endcount,offline,python3,totalcount,serverid,domainname,error_description,cookiestart,cookieend,apitokenid) 
        select source_id,script,threadcount,machines,createddate,dtmodified,developerid,changecount,runstatus,processid,_sid,_eid,modifierid,modifiedfor,lastrun,_machine,input,output,db,status,proxyid,"Server" ,s3offline,msdb,crawltype,endcount,offline,python3,_avgcount,serverid,domainname,error_description,cookiestart,cookieend,apitokenid from pyscripts where processid=_pid and source_id=_webcode limit 1;
    end if;
    if _split1=_split and _split!=1 then 
       insert into pyscripts(source_id,script,threadcount,machines,createddate,dtmodified,developerid,changecount,runstatus,processid,startid,endid,modifierid,modifiedfor,lastrun,machineip,input,output,db,status,proxyid,"Server" ,s3offline,msdb,crawltype,endcount,offline,python3,totalcount,serverid,domainname,error_description,cookiestart,cookieend,apitokenid) 
        select source_id,script,threadcount,machines,createddate,dtmodified,developerid,changecount,runstatus,processid,_sid,_eid,modifierid,modifiedfor,lastrun,_machine,input,output,db,status,proxyid,"Server" ,s3offline,msdb,crawltype,endcount,offline,python3,_avgcount,serverid,domainname,error_description,cookiestart,cookieend,apitokenid from pyscripts where processid=_pid and source_id=_webcode limit 1;
    end if;
    --      i:=i+1;
--      sid := eid+1;
    _lookid=_lookid+_avgcount;
    _split1:=_split1+1;
end loop;

delete FROM pyscripts WHERE id IN 
(SELECT id FROM 
  (SELECT id, ROW_NUMBER() OVER 
    (partition BY processid,source_id,machineip ORDER BY id) AS rnum 
  FROM pyscripts where processid=_pid and source_id=_webcode) t
WHERE t.rnum > 1);

RAISE NOTICE 'Procedure completed';


END;  
$procedure$
;
