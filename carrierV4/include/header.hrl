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
%%-record(dirmeta,{id, filename, createT, modifyT,tag,parent}).


%% dataserver process info
-record(hostinfo,{hostname,nodename,freespace, totalspace, status,life}).
%% chunk mapping relationship
-record(chunkmapping, {chunkid, chunklocations}).
-record(clientinfo, {clientid, modes}).   % maybe fileid?
-record(orphanchunk,{chunkid,chunklocation}).
-record(metalog,{logtime,logfunc,logarg}).

-record(chunkmeta, {chunkid, md5, chunksize}).

-record(garbageinfo, {chunk_id, insert_time}).
-define(GARBAGE_AUTO_COLLECT_PERIOD,	7000).    % 5000 milisecond = 5 second
-define(BOOT_REPORT_RETRY_PERIOD,	7000).    % 5000 milisecond = 5 second
-define(HEART_BEAT_REPORT_WAIT_TIME,	3000).    % 5000 milisecond = 5 second
-define(GARBAGE_AUTO_COLLECT_WAIT_TIME,	7000).    % 5000 milisecond = 5 second

-define(MD5CHECK_TIMER,	86400000).    % 5000 milisecond = 5 second
-define(SPACEREPORT_TIMER,	300000).  % 300000 ms = 5 mins?
-define(HEARTBEAT_TIMER,	10000).
-define(NODE_CHECK_INTERVAL,10000). %% 10second. chech monitor_node result.
-define(CHUNKMAPPING_BROADCAST_INTERVAL,86400000).  %%1day = 24h = 24*3600s = 1000*24*3600 = 86,400,000
-define(HOSTLIFE_AUTO_DECREASE_INTERVAL,5000).
-define(HOST_INIT_LIFE,5).

%%-record(metaWorkerState,{filemeta=#filemeta{},mod,clients=[]}).

%% old file metadata 
%%-record(filemeta, {fileid, filename, filesize, chunklist, createT, modifyT,tag,parent}).

%%-record(filemeta,{id,name,chunklist,parent,size,type,access,atime,mtime,ctime,mode,links,inode,uid,gid})

-record(filemeta,
	{
     %% old one
     id = 0,
     name = 0,
     chunklist =[],
     parent = 0,
    
     %% from file.hrl
     size = 0,			% Size of file in bytes.
	 type = regular,		% Atom: device, directory, regular,
					% or other.
	 access = read,			% Atom: read, write, read_write, or none.
	 atime = {{1970,1,1},{8,0,0}},			% The local time the file was last read:
					% {{Year, Mon, Day}, {Hour, Min, Sec}}.
	 mtime = 0,			% The local time the file was last written.
	 ctime = 0,			% The interpreation of this time field
					% is dependent on operating system.
					% On Unix it is the last time the file or
					% or the inode was changed.  On Windows,
					% it is the creation time.
	 mode = 0,			% Integer: File permissions.  On Windows,
		    			% the owner permissions will be duplicated
					% for group and user.
	 links = 1,			% Number of links to the file (1 if the
					% filesystem doesn't support links).
	 inode = 0,			% Inode number for file.
	 uid = 0,			% User id for owner (integer).
	 gid = 0		% Group id for owner (integer).
    
    }).			
