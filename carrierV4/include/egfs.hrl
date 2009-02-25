%% display debug info or not
-define(debug,"YES").

-ifdef(debug).
-define(DEBUG(Fmt, Args), io:format(Fmt, Args)).
-else.
-define(DEBUG(Fmt, Args), no_debug).
-endif.

%%defien global server name
-define(META_SERVER,{global, global_metaserver}).
-define(HOST_SERVER,{global, global_hostserver}).


%% define Chunk Size and Strip Size
-define(CHUNKSIZE, 33554432).%32*1024*1024

%% version
-define(VERSION,                  16#0001).

%% status code definition
-define(CODE_OK,                  16#0000).
-define(CODE_BADPACKET,           16#0001).
-define(CODE_BADCLIENT,           16#0002).
-define(CODE_BADVERSION,          16#0003).
-define(CODE_OTHERROR,            16#0004).
-define(CODE_SERVERROR,           16#0005).
-define(CODE_NOKEY,               16#0006).

%% type definition

-define(BYTE,                     8/unsigned-big-integer).
-define(WORD,                     16/unsigned-big-integer).
-define(DWORD,                    32/unsigned-big-integer).

-define(CFILE, "/tmp/ctlfile").


-record(chunkmeta, {chunk_id, file_id, path, length, create_time, modify_time}).
-record(garbageinfo, {chunk_id, insert_time}).



%^% meta server  -------------------------------------------


%tables  record to create table.

-record(filemeta, {fileid, filename, filesize, chunklist, createT, modifyT,acl}).
-record(filemeta_s, {fileid, filename, filesize, chunklist, createT, modifyT, acl}).
-record(chunkmapping, {chunkid, chunklocations}).
-record(clientinfo, {clientid, modes}).   % maybe fileid?


-record(orphanchunk,{chunkid,chunklocation}).

-record(metalog,{logtime,logfunc,logarg}).


%% config record
%% procname={RegName, Node()}; host=IP; health={TimeStamp, HealthDegree}

-record(hostinfo,{hostname, freespace, totalspace, status}).

-record(config,{
    access = [{tcp,{{127,0,0,1},51206,512,128}}],
    datafile = "xbtdata",
    users = [{"xbay","xbaytable"}],
    bootnodes = [],
    storage_mod = none
}).

-define(GARBAGE_COLLECT_PERIOD, 1000*60*60*24).    % do collect every day.
-define(HEART_BEAT_PERIOD,		5000).    % 5000 milisecond = 5 second
-define(HEART_BEAT_TIMEOUT,		5*?HEART_BEAT_PERIOD).		
-define(INIT_NODE_HEALTH,		5000). % 5 means a healthy connected data node, 0 means a disconnected ndoe

-define(FILE_WRITE_SHADOW_TABLE,	fwst).
