create table ALL_TRUNC8_PERMS (
  username VARCHAR2(25),
  ownername VARCHAR2(25),
  tablename VARCHAR2(45)
);
-- Where username = the user to audit/manage with the "DROP ANY" system privilege,
-- ownername = the owner of the table that the user is permitted to truncate
-- tablename = the name of the table that the user is permitted to truncate
