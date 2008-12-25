%% display debug info or not
-define(debug,"YES").

-ifdef(debug).
-define(DEBUG(Fmt, Args), io:format(Fmt, Args)).
-else.
-define(DEBUG(Fmt, Args), no_debug).
-endif.

%%defien global server name
-define(DATAGENSERVER,{global, data_server}).
-define(METAGENSERVER,{global, metagenserver}).

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

%% define Chunk Size and Strip Size
-define(CHUNKSIZE, 33554432).%32*1024*1024


%% type definition

-define(BYTE,                     8/unsigned-big-integer).
-define(WORD,                     16/unsigned-big-integer).
-define(DWORD,                    32/unsigned-big-integer).

%% config record
-record(config,{
    access = [{tcp,{{127,0,0,1},51206,512,128}}],
    datafile = "xbtdata",
    users = [{"xbay","xbaytable"}],
    bootnodes = [],
    storage_mod = none
}).

%% crontol file
-define(CFILE, "/tmp/ctlfile").

