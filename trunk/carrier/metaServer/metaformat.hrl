

%tables  record to create table.

-record(filemeta, {fileid, filename, filesize, chunklist, createT, modifyT, acl}).
-record(filemeta_s, {fileid, filename, filesize, chunklist, createT, modifyT, acl}).
-record(chunkmapping, {chunkid, chunklocations}).

-record(clientinfo, {clientid, modes}).   % maybe fileid?
-record(filesession, {fileid, client}).
-record(hostinfo,{ip,host,freespace,totalspace}).

-record(orphanchunk,{chunkid,chunklocation}).

-record(metalog,{logtime,logfunc,logarg}).
