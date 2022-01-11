-- This trigger first requires the following tables to already have been created:
-- ALL_TRUNC8_PERMS
-- CAPTURED_TRUNC8_ERRS
create or replace trigger sys.truncate_table_pass
before truncate
on database
declare
   v_db_name varchar2(64);
   v_owner_authrzd varchar2(35);
   v_owner_tried varchar2(35);
   v_table varchar2(35);
   v_text varchar2(32767);
   v_user varchar2(35);
begin
   v_user := user;
   v_table := ora_dict_obj_name;
   v_owner_tried := ora_dict_obj_owner;
   SELECT value INTO v_db_name
   FROM v$parameter
   WHERE name = 'db_name';   
   if v_user = '<USER_NAME>' then
      if ora_sysevent = 'TRUNCATE' and v_table not in (select table_name from ALL_TRUNC8_PERMS where user='<USER_NAME>')
      then 
          raise_application_error(-20001,'TRUNCATE not permitted: Insufficient user authorized permission');
	  insert into CAPTURED_TRUNC8_ERRS VALUES (SYSDATE, v_user, v_owner_tried, v_table);
          v_text := SYSDATE || ': ' || v_user || ' attemped an unathorized TRUNCATE TABLE on ' || v_owner_tried || '.' || v_table || ' in ' || v_db_name || '.';
          Mail_Pkg.send ( p_sender_email => '<EMAIL ADDRESS>',
                                        p_from => v_db_name,
                                        p_to => Mail_Pkg.array( '<EMAIL ADDRESS>'),
                                        p_subject => 'UNAUTHORIZED USER ACTION in '||v_db_name,
                                        p_body => v_text); 
      else if ora_sysevent = 'TRUNCATE' and v_table in (select table_name from ALL_TRUNC8_PERMS) 
      then
          SELECT owner INTO v_owner_authrzd
          FROM ALL_TRUNC8_PERMS
          WHERE user='<USER_NAME>'
            and table_name=v_table;
          if (v_owner_tried <> v_owner_authrzd)
          then  
              raise_application_error(-20001,'TRUNCATE not permitted: Insufficient user authorized permission');
	      insert into CAPTURED_TRUNC8_ERRS VALUES (SYSDATE, v_user, v_owner_tried, v_table);
              v_text := SYSDATE || ': ' || v_user || ' attemped an unathorized TRUNCATE TABLE on ' || v_owner_tried || '.' || v_table || ' in ' || v_db_name || '.';
              Mail_Pkg.send ( p_sender_email => '<EMAIL ADDRESS>',
                                            p_from => v_db_name,
                                            p_to => Mail_Pkg.array( '<EMAIL ADDRESS>'),
                                            p_subject => 'UNAUTHORIZED USER ACTION in '||v_db_name,
                                            p_body => v_text);  
          end if; 
      end if;
   end if;
end;
