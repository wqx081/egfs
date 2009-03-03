

%tables  record for access control list.

-record(aclrecord, {filename, userlist, grouplist, otherlist}).
-record(grouprecord, {groupname, grouplist}).
-record(acllog,{logtime,logfunc,logarg}).
