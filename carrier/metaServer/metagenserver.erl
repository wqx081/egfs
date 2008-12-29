-module(metagenserver).
-behaviour(gen_server).

-include("../include/egfs.hrl").

-import(metaserver, [do_open/3, 
                     do_allocate_chunk/2, 
                     do_register_chunk/4, 
                     do_get_chunk/2, 
                     do_close/2,
                     do_dataserver_bootreport/2,
                     do_delete/1]).

-export([start/0,stop/0,terminate/2]).
-export([init/1, handle_call/3, handle_cast/2,handle_info/2]).
-export([open/2,writeAllocate/1,locateChunk/2,registerChunk/4,close/1]).
-compile(export_all).

-define(GM,{global, metagenserver}).

init(_Arg) ->
    process_flag(trap_exit, true),
    io:format("meta server starting~n"),
    %init_mnesia(),
    {ok, []}.

start() ->
    metaDB:start_mnesia(),
    gen_server:start_link(?GM, metagenserver, [], []).

stop() ->
    gen_server:cast(?GM, stop).

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

handle_call({delete,FileId},{_From,_},State) ->
    io:format("inside handle_call_delete,FileID:~p~n",[FileId]),
	Reply = do_delete(FileId),
    {reply, Reply, State};

handle_call({bootreport,HostInfoRec, ChunkList},{_From,_},State) ->
    io:format("inside handle_call_bootreport,FileID:~p~n",[HostInfoRec]),
	Reply = do_dataserver_bootreport(HostInfoRec, ChunkList),
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
    gen_server:call(?GM, {open, FileName,Mode}).

open2(FileName,Mode) ->
    gen_server:call(?GM,{open2,FileName,Mode}).

writeAllocate(FileID) ->
    gen_server:call(?GM, {allocatechunk, FileID}).

locateChunk(FileID, ChunkIndex)->
    gen_server:call(?GM, {locatechunk, FileID, ChunkIndex}).

close(FileID)->
    gen_server:call(?GM, {close, FileID}).

delete(FileID)->
    gen_server:call(?GM, {delete, FileID}).

%% "chunk server api" methods
%% write operation
%% 3. registerChunk(chunkserver) =>

%% boot report methods
%% 1. bootReport(HostInfoRec, ChunkList)

registerChunk(FileID, ChunkID, ChunkUsedSize, NodeList)->
    gen_server:call(?GM, {registerchunk, FileID, ChunkID, ChunkUsedSize, NodeList}).

%%
%% bootReport(HostInfoRec, ChunkList) => {ok, OrphanChunkList} | {error, "Reason for error"}
bootReport(HostInfoRec, ChunkList)->
    gen_server:call(?GM, {bootreport, HostInfoRec, ChunkList}).

