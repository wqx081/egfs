-module(metagenserver).
-behaviour(gen_server).

-include("../../include/egfs.hrl").

-import(metaserver, [do_open/3, 
                     do_allocate_chunk/2, 
                     do_register_chunk/4, 
                     do_get_chunk/2, 
                     do_close/2,
                     do_dataserver_bootreport/2,
                     do_delete/2,
                     do_collect_orphanchunk/1,
                     do_get_fileattr/1,
                     do_register_heartbeat/1]).

-export([start/0,stop/0,terminate/2]).
-export([init/1, handle_call/3, handle_cast/2,handle_info/2]).
-export([open/2,writeAllocate/1,locateChunk/2,registerChunk/4,close/1]).
-compile(export_all).

%-define(GM,{global, metagenserver}).

init(_Arg) ->
    process_flag(trap_exit, true),
    io:format("meta server starting~n"),
    %init_mnesia(),
    {ok, []}.

die() ->
    1000 div 0.

start() ->
    metaDB:start_mnesia(),
%%     {ok,Tref} = timer:apply_interval((?HEART_BEAT_TIMEOUT),hostMonitor,checkHostHealth,[]), % check host health every 5 second
    gen_server:start_link(?META_SERVER, metagenserver, [], []).

stop() ->
    gen_server:cast(?META_SERVER, stop).

terminate(_Reason, _State) ->
    io:format("meta server terminating~n").


%"metaserver" methods
% write step 1: open file
handle_call({open, FileName, Mode}, {From, _}, State) ->
    io:format("inside handle_call_open, FileName:~p,Mode:~p,From:~p~n",[FileName,Mode,From]),
    Reply = do_open(FileName, Mode, From),
    {reply, Reply, State};

handle_call({allocatechunk, FileID}, {From, _}, State) ->
    io:format("inside handle_call_allocatechunk, FileID:~p,From:~p~n",[FileID,From]),
    Reply = do_allocate_chunk(FileID, From),
    {reply, Reply, State};

%% --------------------------------------------------------------------
%% Function:
%% Arg     :		NodeList must be a list
%% Description: 
%% Returns: 
%% --------------------------------------------------------------------
handle_call({registerchunk, FileID, ChunkID, ChunkUsedSize, NodeList},
            _From, State) ->    
    io:format("~p",[time()]),
    io:format("inside handle_call_registerchunk,FileID:~p,ChunkID:~p,ChunkUsedSize:~p~n",[FileID,ChunkID,ChunkUsedSize]),
    io:format("NodeList~p~n",[list_to_tuple(NodeList)]),
    Reply = do_register_chunk(FileID, ChunkID, ChunkUsedSize, NodeList),
    {reply, Reply, State};

handle_call({locatechunk, FileID, ChunkIndex}, _From, State) ->
    io:format("inside handle_call_locatechunk,FileID:~p,ChunkIndex:~p~n",[FileID,ChunkIndex]),
    Reply = do_get_chunk(FileID, ChunkIndex),
    {reply, Reply, State};

handle_call({close, FileID}, {From, _}, State)->
    io:format("inside handle_call_close,FileID:~p,From:~p~n",[FileID,From]),
	Reply = do_close(FileID, From),
    {reply, Reply, State};


handle_call({delete,FileName},{From,_},State) ->
    io:format("inside handle_call_delete,FileName:~p~n",[FileName]),
	Reply = do_delete(FileName, From),
    {reply, Reply, State};

handle_call({bootreport,HostInfoRec, ChunkList},{_From,_},State) ->
    io:format("inside handle_call_bootreport,HostInfoRec:~p~n",[HostInfoRec]),
	Reply = do_dataserver_bootreport(HostInfoRec, ChunkList),
    {reply, Reply, State};

handle_call({getorphanchunk, HostRegName},{_From,_},State) ->
    io:format("inside handle_call_getorphanchunk,HostRegName:~p~n",[HostRegName]),
	Reply = do_collect_orphanchunk(HostRegName),
    {reply, Reply, State};

handle_call({getfileattr, FileName},{_From,_},State) ->
    io:format("inside handle_call_getfileattr,FileName:~p~n",[FileName]),
	Reply = do_get_fileattr(FileName),
    {reply, Reply, State};


handle_call({heartbeat,HostInfoRec},{_From,_},State) ->
  %  io:format("inside handle_call_heartbeat,HostRegName:~p~n",[HostInfoRec]),
	Reply = do_register_heartbeat(HostInfoRec),
    {reply, Reply, State};


handle_call(_, {_From, _}, State)->
    io:format("inside handle_call_error~n"),
	Reply = {error, "undefined handler"},
    {reply, Reply, State}.


handle_cast(stop, State) ->
    io:format("meta server stopping~n"),
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%%"client api" methods
% write operation sequence:
% 1. open(client) =>
% 2. writeAllocate(client) =>
% ****3. registerChunk(chunkserver) =>
% 4. close(client)

% read operation sequence:
% 1. open(client) =>
% 2. locateChunk(client) =>
% 3. close(client)

%% delete operation:
%% 1. delete(FileID)

open(FileName,Mode) ->
    gen_server:call(?META_SERVER, {open, FileName,Mode}).

open2(FileName,Mode) ->
    gen_server:call(?META_SERVER,{open2,FileName,Mode}).

writeAllocate(FileID) ->
    gen_server:call(?META_SERVER, {allocatechunk, FileID}).

locateChunk(FileID, ChunkIndex)->
    gen_server:call(?META_SERVER, {locatechunk, FileID, ChunkIndex}).

close(FileID)->
    gen_server:call(?META_SERVER, {close, FileID}).

delete(FileID)->
    gen_server:call(?META_SERVER, {delete, FileID}).

%% "chunk server api" methods
%% write operation
%% 3. registerChunk(chunkserver) =>

%% boot report methods
%% 1. bootReport(HostInfoRec, ChunkList)

registerChunk(FileID, ChunkID, ChunkUsedSize, NodeList)->
    gen_server:call(?META_SERVER, {registerchunk, FileID, ChunkID, ChunkUsedSize, NodeList}).

%%
%% bootReport(HostInfoRec, ChunkList) => {ok, OrphanChunkList} | {error, "Reason for error"}
bootReport(HostInfoRec, ChunkList)->
    gen_server:call(?META_SERVER, {bootreport, HostInfoRec, ChunkList}).

%%
%% garbage collection methods
%% get orphan chunks
%% 1. getOrphanChunk(HostRegName) => [<<OrphanChunkID:64>>, ...]
getOrphanChunk(HostProcName)->
    gen_server:call(?META_SERVER, {getorphanchunk, HostProcName}).

%%
%% get file attributes methods
%% 1. open file to get FileID
%% 2. getFileAttr(FileID) => {ok, [FileSize, <<CreateTime:64>>, <<ModifyTime:64>>, ACL]}
getFileAttr(FileID)->
    gen_server:call(?META_SERVER, {getfileattr, FileID}).


heartBeat(HostInfoRec) ->
    gen_server:call(?META_SERVER,{heartbeat,HostInfoRec}).

