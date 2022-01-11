-- This script copies a very large table across a database link in batches (chunk_size == how many rows to copy over in each batch)
-- This is to be ran after the source table's metadata has been cloned in the target database
-- The following database table needs to already have been created in order for this script to successfully execute: DB_MIGRATION_TRACKER
-- The database link needs to already have been created in order for this script to successfully execute
-- author: Beth Culler 
DECLARE
  v_Chunk_Size NUMBER := 500;
  v_towner VARCHAR2(50) := '<OWNER of LARGE TABLE TO MIGRATE>';
  v_tname VARCHAR2(50) := '<LARGE TABLE TO MIGRATE>';
  v_fq_table VARCHAR2(50) := v_towner || '.' || v_tname;
  v_db_name VARCHAR2(64);
  v_hostname VARCHAR2(64);
  v_minrow VARCHAR2(64);
  v_maxrow VARCHAR2(64);
  v_1start_time DATE;
  v_start_time DATE;
  v_end_time DATE;
  v_elapsed_time integer;
  v_text VARCHAR2(32767);
  v_sql_text varchar2(32767);
  v_up VARCHAR2(32767);
  v_update VARCHAR2(32767);
-- This function allows you to monitor the migration process. Querying this table while this script is running provides a glimpse into where in the migration the script is at
FUNCTION insertUpdate (v_update IN VARCHAR2)
RETURN VARCHAR2
IS
  updatenote VARCHAR2(32767);
BEGIN 
  insert into db_migration_tracker VALUES(SYSDATE, '<MIGRATION NAME>', v_update);
  commit;
  updatenote := 'Inserted update into SYS.db_migration_tracker';
  RETURN updatenote;
END;
BEGIN
  SELECT value INTO v_db_name
  FROM v$parameter
  WHERE name = 'db_name';
  SELECT distinct machine INTO v_hostname
  FROM v$session;
  v_1start_time := SYSDATE;
  v_text := null;
  v_text := v_text || v_1start_time || ' : Starting migration of ' || v_fq_table || CHR(10);
  v_text := v_text || 'Disabling FK constraints...' || CHR(10);
  dbms_output.put_line(v_1start_time || ' : Starting migration of ' || v_fq_table);
  dbms_output.put_line('Disabling FK constraints...');
  v_update := 'Started disabling FK constraints';
  v_up := insertUpdate(v_update);
  dbms_output.put_line(v_up);
-- Disable FK constraints on the table
  FOR c IN
  (SELECT c.owner, c.table_name, c.constraint_name
   FROM dba_constraints c, dba_tables t
   WHERE c.table_name = t.table_name
   AND c.table_name = v_tname
   AND c.owner = v_towner
   AND  c.constraint_type = 'R'
   ORDER BY c.constraint_name)
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
  v_text := v_text || 'Complete. Total elapsed time to disable FK constraints = ' || v_elapsed_time || ' hours.' || CHR(10);
  dbms_output.put_line('Complete. Total elapsed time to disable FK constraints = ' || v_elapsed_time || ' hours.');
  v_update := 'Finished disabling all Foreign Keys';
  v_up := insertUpdate(v_update);
  dbms_output.put_line(v_up);
  v_start_time := SYSDATE;
  v_text := v_text || v_start_time || ' : Copying over data...' || CHR(10);
  dbms_output.put_line(v_start_time || ' : Copying over data...');
  FOR v_Range IN (
   WITH splits as
   (
   SELECT
     ROW_NUMBER() OVER (ORDER BY rowid) AS Row_Num,
     CEIL(ROW_NUMBER() OVER (ORDER BY rowid)/v_Chunk_Size) AS Batch_Number,
     ROWID AS RowID1 
   FROM
     <TABLE OWNER>.<TABLE NAME>@<DB LINK NAME>
   )
   SELECT
     Batch_Number, 
     MIN(RowID1) AS MinRowID,
     MAX(ROWID1) AS MaxRowID, 
     COUNT(1) AS Batch_Row_Count 
   FROM
     splits
   GROUP BY
     Batch_Number
   ORDER BY
     Batch_Number) 
  LOOP
    DBMS_OUTPUT.PUT_LINE('BATCH: ' || v_Range.Batch_Number);
    DBMS_OUTPUT.PUT_LINE('Copying over MIN to MAX ROWID range: ' || v_Range.MinRowID || ', ' || v_Range.MaxRowID);
    v_update := 'Starting BATCH: ' || v_Range.Batch_Number || ' - Copying over MIN to MAX ROWID range: ' || v_Range.MinRowID || ', ' || v_Range.MaxRowID;
    v_up := insertUpdate(v_update); 
    dbms_output.put_line(v_up);
    v_start_time := SYSDATE;
    v_sql_text := 'insert into ' || v_fq_table ||'(select * from ' || v_fq_table || '@<DB LIBK NAME> where rowid between '''||V_Range.MinRowID||''' and '''||V_Range.MaxRowID||''')';
    execute immediate v_sql_text;
    COMMIT;
    v_end_time := SYSDATE;
    -- Calculate the time it took to copy over the data in hours
    select 24 * (v_end_time - v_start_time) INTO v_elapsed_time from dual;
    dbms_output.put_line('Complete. Total elapsed time for BATCH ' || v_Range.Batch_Number || ' = ' || v_elapsed_time || ' hours.');
    v_update := 'Finished BATCH:  ' || v_Range.Batch_Number;
    v_up := insertUpdate(v_update);
    dbms_output.put_line(v_up);
  END LOOP;
  v_end_time := SYSDATE;
  -- Calculate the time it took from start to finish 
  select 24 * (v_end_time - v_1start_time) INTO v_elapsed_time from dual;
  v_text := v_text || v_end_time || ' : MIGRATION COMPLETE' || CHR(10);
  v_text := v_text || 'Total elapsed time from start to finish for migration = ' || v_elapsed_time || ' hours.' || CHR(10);
  dbms_output.put_line(v_end_time || ' : MIGRATION COMPLETE');
  dbms_output.put_line('Total elapsed time from start to finish for migration = ' || v_elapsed_time || ' hours.');
  v_update := 'MIGRATION COMPLETE. TOTAL TIME TO COMPLETE = ' || v_elapsed_time || ' hours.';
  v_up := insertUpdate(v_update);
  dbms_output.put_line(v_up);
  Mail_Pkg.send (p_sender_email => '<EMAIL ADDRESS>',
                 p_from => v_db_name || '@' || v_hostname,
                 p_to => Mail_Pkg.array('<EMAIL ADDRESS>'),
                 p_subject => v_fq_table || ' MIGRATION REPORT',
                 p_body => v_text);  
END;
/
