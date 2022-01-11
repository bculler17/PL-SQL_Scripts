-- This script copies the table data from a schema in one database (the source database) to an identical schema in another database (the target database) across a database link.
-- This is to be ran after the source schema's metadata has been cloned in the target database.
-- The following database table needs to already have been created in order for this script to successfully execute: DB_MIGRATION_TRACKER
-- The database link needs to already have been created in order for this script to successfully execute
-- author: Beth Culler
DECLARE
  v_schema_name VARCHAR2(25) := '<SCHEMA NAME>';
  v_excluded_table VARCHAR2(25) := '<A PERHAPS LARGE TABLE TO BE EXCLUDED TO INSTEAD MIGRATE OVER LATER ON ITS OWN USING MY migrate_large_table-DBLINK.sql SCRIPT>';
  v_dblink VARCHAR2(20) := '<DBLINK NAME>';
  v_db_name varchar2(64);
  v_hostname VARCHAR2(64);
  v_text varchar2(32767);
  v_sql_text varchar2(32767);
  v_1start_time date;
  v_start_time date;
  v_end_time date;
  v_tstart_time date;
  v_tend_time date;
  v_elapsed_time integer;
  v_elapsed_ftime integer; 
  tables integer;
  v_tb VARCHAR2(50);
  v_up VARCHAR2(32767);
  v_update VARCHAR2(32767);
-- This function allows you to monitor the migration process. Querying this table while this script is running provides a glimpse into where in the migration the script is at
FUNCTION insertUpdate (v_update IN VARCHAR2)
RETURN VARCHAR2
IS
  updatenote VARCHAR2(32767);
BEGIN
  insert into dbtesting_monitoring VALUES(SYSDATE, 'NEW_SCHEMA_MIGRATION', v_update);
  commit;
  updatenote := 'Inserted update into SYS.dbtesting_monitoring';
  RETURN updatenote;
END;
BEGIN
   SELECT value INTO v_db_name
   FROM v$parameter
   WHERE name = 'db_name';
   SELECT distinct machine INTO v_hostname
   FROM v$session;
-- Disable the FK constraints on all of the new v_schema_name tables in the new target db
  v_1start_time := SYSDATE;
  v_text := null;
  v_text := v_text || v_schema_name || ' Migration Over Database Link:' || CHR(10);
  v_text := v_text || v_1start_time || ' : BEGAN DISABLING FK CONSTRAINTS IN TARGET DB..' || CHR(10);  
  dbms_output.put_line(v_text);
  v_update := 'Started disabling FK constraints';
  v_up := insertUpdate(v_update);
  dbms_output.put_line(v_up);
  -- Constraints
  FOR c IN
  (SELECT c.owner, c.table_name, c.constraint_name
   FROM dba_constraints c, dba_tables t
   WHERE c.table_name = t.table_name
   AND c.table_name <> v_excluded_table
   AND c.owner = v_schema_name
   AND  c.constraint_type = 'R'
   ORDER BY c.table_name, c.constraint_name)
  LOOP
    -- DISABLE
    execute immediate 'alter table "' || c.owner || '"."' || c.table_name || '" disable constraint ' || c.constraint_name;
    v_update := 'Disabled ' || c.constraint_name;
    v_up := insertUpdate(v_update);
    dbms_output.put_line(v_up);
  END LOOP;
  v_end_time := SYSDATE;
-- Calculate the time it took to disable all of the FK constraints in hours
  select 24 * (v_end_time - v_1start_time) INTO v_elapsed_time from dual;
  v_text := v_text || v_end_time || ' : FINISHED DISABLING FK CONSTRAINTS.' || CHR(10);
  v_text := v_text || 'Total elapsed time to disable FK constraints = ' || v_elapsed_time || ' hours.' || CHR(10);   
  dbms_output.put_line('FK constraints disabled..');
  v_update := 'Finished disabling all Foreign Keys';
  v_up := insertUpdate(v_update);
  dbms_output.put_line(v_up);
-- Copy data in each v_schema_name table from source db to target db  
  v_start_time := SYSDATE;
  v_text := v_text || v_start_time || ' : BEGAN COPYING ' || v_schema_name || ' DATA OVER THE DB LINK..' || CHR(10);
  dbms_output.put_line('Starting to copy tables over the db link..');
  FOR d IN
  (SELECT d.table_name
    FROM dba_tables d
    WHERE d.owner= v_schema_name
    AND d.table_name <> v_excluded_table
    order by d.table_name)
  LOOP
    -- COPY
     v_update := 'Started to copy ' || v_schema_name || '"."' || d.table_name;
     v_up := insertUpdate(v_update);
     dbms_output.put_line(v_up);
     v_tstart_time := SYSDATE;
     execute immediate 'insert into "' || v_schema_name || '"."' || d.table_name || '" (select * from "' || v_schema_name || '"."' || d.table_name || '"@<DBLINK NAME>)';
     commit;
     v_tend_time := SYSDATE;
-- Calculate the time it took to copy over the data for this table in hours
     select 24 * (v_tend_time - v_tstart_time) INTO v_elapsed_time from dual;
     v_text := v_text || 'Total elapsed time for ' || v_schema_name || '.' || d.table_name || ' = ' || v_elapsed_time || ' hours.' || CHR(10); 
     dbms_output.put_line('Finished copying ' || v_schema_name || '.' || d.table_name || '.');
     v_update := 'Finished copying ' || v_schema_name || '"."' || d.table_name;
     v_up := insertUpdate(v_update);
     dbms_output.put_line(v_up);
  END LOOP;
  v_end_time := SYSDATE;
-- Calculate the total time it took to copy over the data for all of the tables in hours
  select 24 * (v_end_time - v_start_time) INTO v_elapsed_time from dual;
  v_text := v_text || v_end_time || ' : FINISHED COPYING DATA OVER THE DB LINK.' || CHR(10);
  v_text := v_text || 'Total elapsed time for db link migration = ' || v_elapsed_time || ' hours.' || CHR(10);   
  dbms_output.put_line('Finished copying the last table. Schema migration is FINISHED.');
  v_update := 'MIGRATION COMPLETE. TOTAL TIME TO COMPLETE = ' || v_elapsed_time || ' hours.';
  v_up := insertUpdate(v_update);
  dbms_output.put_line(v_up);
-- Re-enable the referential constraints on all of the new v_schema_name tables in the new target db
  v_start_time := SYSDATE;
  v_text := v_text || v_start_time || ' : BEGAN RE-ENABLING FK CONSTRAINTS IN TARGET DB..' || CHR(10);
  dbms_output.put_line('Re-enabling constraints..');
  v_update := 'Started enabling FK constraints';
  v_up := insertUpdate(v_update);
  dbms_output.put_line(v_up);
  -- Constraints
  FOR c IN
  (SELECT c.owner, c.table_name, c.constraint_name
   FROM dba_constraints c, dba_tables t
   WHERE c.table_name = t.table_name
   AND c.owner = v_schema_name
   AND  c.constraint_type = 'R'
   ORDER BY c.table_name, c.constraint_name)
  LOOP
    -- ENABLE
    execute immediate 'alter table "' || c.owner || '"."' || c.table_name || '" enable constraint ' || c.constraint_name;
    v_update := 'Enabled ' || c.constraint_name;
    v_up := insertUpdate(v_update);
    dbms_output.put_line(v_up);
  END LOOP;
  v_end_time := SYSDATE;
-- Calculate the time it took to re-enable all of the FK constraints in hours
  select 24 * (v_end_time - v_start_time) INTO v_elapsed_time from dual;
-- Calculate the total time the migration took from start to finish in hours
  select 24 * (v_end_time - v_1start_time) INTO v_elapsed_ftime from dual;
  v_text := v_text || v_end_time || ' : FINISHED RE-ENABLING FK CONSTRAINTS.' || CHR(10);
  v_text := v_text || 'Total elapsed time to re-enable FK constraints = ' || v_elapsed_time || ' hours.' || CHR(10);
  v_text := v_text || v_schema_name || ' Migration IS COMPLETE.' || CHR(10);
  v_text := v_text || 'Total elapsed time for migration from start to finish = ' || v_elapsed_ftime || ' hours.' || CHR(10);  
  dbms_output.put_line('Constraints have been re-enabled. Project is complete. Check email. Good-bye!');
  v_update := 'Finished enabling all Foreign Keys. ENTIRE DATABASE MIGRATION COMPLETE.';
  v_up := insertUpdate(v_update);
  dbms_output.put_line(v_up);
  Mail_Pkg.send (p_sender_email => '<EMAIL ADDRESS>',
                 p_from => v_db_name || '@' || v_hostname,
                 p_to => Mail_Pkg.array('<EMAIL ADDRESS>'),
                 p_subject => v_schema_name || ' MIGRATION REPORT',
                 p_body => v_text);   
END;
/
