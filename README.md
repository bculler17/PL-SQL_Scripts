# PLSQL_Scripts
Collection of PL/SQL scripts that I have written that I use for Oracle database management 

These are various PL/SQL scripts I have written to help me manage Oracle 11g, 12c, and 19c databases on AIX and Linux systems.

1. [truncate_permitting_trigger.sql](/triggers/truncate_permitting_trigger.sql) : A user was required to be able to truncate certain application tables not owned by them, so the user was granted the "DROP ANY TABLE" system privilege (to truncate a table in Oracle, the table must be in your own schema or you must have the "DROP ANY TABLE" system privilege. But this privilege allows you to then truncate or drop every single table in the database). This trigger was written so that the user could only truncate the pre-approved application tables and would be prevented from being able to truncate every table in the database.    
