create table CAPTURED_TRUNC8_ERRS (
  err_timestamp DATE,
  username VARCHAR2(25),
  ownername VARCHAR2(25),
  tablename VARCHAR2(45),
);
-- Where err_timestamp = the time the truncate was attempted
-- username = the user to audit/manage with the "DROP ANY" system privilege,
-- ownername = the owner of the table that the user is not permitted to truncate but attempted to truncate anyway
-- tablename = the name of the table that the user is not permitted to truncate but attempted to truncate anyway
