%%defien global server name
-define(META_SERVER,{global, global_metaserver}).
-define(HOST_SERVER,{global, global_hostserver}).

-define(SERVER_NAME, dataserver).
%% define Chunk Size and Strip Size
%-define(CHUNKSIZE, 33554432). % 32Mbytes, 32*1024*1024
%-define(STRIP_SIZE, 8192).
-define(CHUNKSIZE, 33554432). % 32Mbytes, 32*1024*1024


-define(DATA_PORT, 7777).
-define(MAX_CONN, 100).
-define(PASSWORD,"carrier").
-define(DATA_PREFIX,"./Chunks.").
-define(TCP_OPTIONS, [binary, {packet, 2}, {active, once}, {nodelay,true}, {reuseaddr, true}]).
-define(ACTIVE_OPTIONS, [{active, once}]).
%% version
-define(VERSION, 16#0001).

-record(filecontext, {fileid, 
					  filename, 
					  filesize, 
					  offset,
					  mode, 
					  metaworkerpid, 
					  dataworkerpid, 
					  chunkid=[],
					  chunklist=[],
					  host=[], 
					  nodelist=[]}).

%% file metadata 
-record(filemeta, {fileid, filename, filesize, chunklist, createT, modifyT, acl}).

%% dataserver process info
-record(hostinfo,{hostname, freespace, totalspace, status}).

%% chunk mapping relationship
-record(chunkmapping, {chunkid, chunklocations}).

 


-record(chunkmeta, {chunk_id, file_id, path, length, create_time, modify_time}).
-record(garbageinfo, {chunk_id, insert_time}).
-define(GARBAGE_AUTO_COLLECT_PERIOD,	7000).    % 5000 milisecond = 5 second
-define(BOOT_REPORT_RETRY_PERIOD,	7000).    % 5000 milisecond = 5 second
-define(HEART_BEAT_REPORT_WAIT_TIME,	3000).    % 5000 milisecond = 5 second
-define(GARBAGE_AUTO_COLLECT_WAIT_TIME,	7000).    % 5000 milisecond = 5 second
