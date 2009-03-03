%%defien global server name
-define(META_SERVER,{global, global_metaserver}).
-define(HOST_SERVER,{global, global_hostserver}).

-define(SERVER_NAME, dataserver).
%% define Chunk Size and Strip Size
%-define(CHUNKSIZE, 33554432). % 32Mbytes, 32*1024*1024
%-define(STRIP_SIZE, 8192).
-define(CHUNKSIZE, 33554432). % 32Mbytes, 32*1024*1024
-define(STRIP_SIZE, 131072). % 128Kbytes, 128*1024

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


%%%
-record(bloom, {
    m      = 0,       % The size of the bitmap in bits.
    bitmap = <<>>,    % The bitmap.
    k      = 0,       % The number of hashes.
    n      = 0,       % The maximum number of keys.
    keys   = 0        % The current number of keys.
}).

%^% meta server  -------------------------------------------


%%naming server.
-record(dirmeta,{id, filename, createT, modifyT,tag,parent}).

%% file metadata 
-record(filemeta, {fileid, filename, filesize, chunklist, createT, modifyT, acl}).
%% dataserver process info
-record(hostinfo,{hostname, freespace, totalspace, status}).
%% chunk mapping relationship
-record(chunkmapping, {chunkid, chunklocations}).
-record(clientinfo, {clientid, modes}).   % maybe fileid?
-record(orphanchunk,{chunkid,chunklocation}).
-record(metalog,{logtime,logfunc,logarg}).

-record(chunkmeta, {chunkid, md5}).

-record(garbageinfo, {chunk_id, insert_time}).
-define(GARBAGE_AUTO_COLLECT_PERIOD,	7000).    % 5000 milisecond = 5 second
-define(BOOT_REPORT_RETRY_PERIOD,	7000).    % 5000 milisecond = 5 second
-define(HEART_BEAT_REPORT_WAIT_TIME,	3000).    % 5000 milisecond = 5 second
-define(GARBAGE_AUTO_COLLECT_WAIT_TIME,	7000).    % 5000 milisecond = 5 second

-define(MD5CHECK_TIMER,	86400000).    % 5000 milisecond = 5 second

-define(NODE_CHECK_INTERVAL,10000). %% 10second. chech monitor_node result.
-define(CHUNKMAPPING_BROADCAST_INTERVAL,86400000)  %%1day = 24h = 24*3600s = 1000*24*3600 = 86,400,000
