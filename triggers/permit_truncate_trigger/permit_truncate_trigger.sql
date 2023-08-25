-- This trigger first requires the following tables to already have been created:
-- ALL_TRUNC8_PERMS
-- CAPTURED_TRUNC8_ERRS
-- Author: Beth Culler
create or replace trigger sys.permit_truncate_trigger
before truncate
on database
declare
   v_db_name varchar2(64);
   v_owner_authrzd varchar2(35);
   v_owner_tried varchar2(35);
   v_table varchar2(35);
   v_text varchar2(32767);
   v_user varchar2(35);
   v_count number;
   v_date varchar2(35);
   PROHIBITEDACTION EXCEPTION;
   Pragma AUTONOMOUS_TRANSACTION;
begin
   v_user := user;
   v_table := ora_dict_obj_name;
   v_owner_tried := ora_dict_obj_owner;
   if v_user = '<USER GRANTED DROP ANY TABLE PRIVILEGE>' and v_owner_tried <> '<USER GRANTED DROP ANY TABLE PRIVILEGE>'
   then
      SELECT TO_CHAR(sysdate, 'DD-MON-YYYY HH24:MI:SS') INTO v_date FROM dual;
      SELECT value INTO v_db_name
      FROM v$parameter
      WHERE name = 'db_name';  
      select count(*)INTO v_count
      from ALL_TRUNC8_PERMS 
      where username='<USER GRANTED DROP ANY TABLE PRIVILEGE>'
        and lower(tablename) = lower(v_table);
      if (ora_sysevent = 'TRUNCATE' and v_count = 0)
      then
          v_text := v_date || ': ' || v_user || ' attemped an unathorized TRUNCATE TABLE on ' || v_owner_tried || '.' || v_table || ' in ' || v_db_name || '.';
          Mail_Pkg.send ( p_sender_email => '<email>',
                          p_from => v_db_name,
                          p_to => Mail_Pkg.array( '<email>'),
                          p_subject => 'UNAUTHORIZED USER ACTION in '||v_db_name,
                          p_body => v_text);
          RAISE PROHIBITEDACTION;
      elsif (ora_sysevent = 'TRUNCATE' and v_count > 0)
      then
          SELECT ownername INTO v_owner_authrzd
          FROM ALL_TRUNC8_PERMS
          WHERE username='<USER GRANTED DROP ANY TABLE PRIVILEGE>'
            and lower(tablename)=lower(v_table);
          if (v_owner_tried <> v_owner_authrzd)
          then  
              v_text := v_date || ': ' || v_user || ' attemped an unathorized TRUNCATE TABLE on ' || v_owner_tried || '.' || v_table || ' in ' || v_db_name || '. ' || v_user || ' is permitted to instead truncate ' || v_owner_authrzd || '.' || v_table || '.';
              Mail_Pkg.send ( p_sender_email => '<email>',
                          p_from => v_db_name,
                          p_to => Mail_Pkg.array( '<email>'),
                          p_subject => 'UNAUTHORIZED USER ACTION in '||v_db_name,
                          p_body => v_text);
              RAISE PROHIBITEDACTION;
          end if; 
      end if;
   end if;
EXCEPTION
        WHEN PROHIBITEDACTION then
          insert into CAPTURED_TRUNC8_ERRS VALUES (sysdate, v_user, v_owner_tried, v_table);
          commit;
          raise_application_error(-20001,'TRUNCATE not permitted - insufficient user authorized permission');
end;
