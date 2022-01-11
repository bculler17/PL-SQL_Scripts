create table DB_MIGRATION_TRACKER (
  JOB_DATE DATE,
  JOB_NAME VARCHAR2(100),
  JOB_UPDATE VARCHAR2(32767)
);
-- where JOB_NAME is any name you would like to call the migration, for example: <schema or table name>_migration_2021
