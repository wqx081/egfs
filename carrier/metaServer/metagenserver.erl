-module(metagenserver).
-behaviour(gen_server).

-include("../include/egfs.hrl").

-import(metaserver, [do_open/3, do_allocate_chunk/2, do_register_chunk/4, do_get_chunk/2, do_close/2]).

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
    gen_server:start_link(?GM, metagenserver, [], []).

stop() ->
    gen_server:cast(?GM, stop).

terminate(Reason, State) ->
    io:format("meta server terminating~n").


%"metaserver" methods
% write step 1: open file
handle_call({open, FileName, Mode}, {From, _}, State) ->
    Reply = do_open(FileName, Mode, From),
    {reply, Reply, State};

handle_call({allocatechunk, FileID}, {From, _}, State) ->
    Reply = do_allocate_chunk(FileID, From),
    {reply, Reply, State};

handle_call({registerchunk, FileID, ChunkID, ChunkUsedSize, NodeList},
            From, State) ->
    ?DEBUG("[ChunkUsedSize]: ChunkUsedSize ~p~n", [ChunkUsedSize]),
    Reply = do_register_chunk(FileID, ChunkID, ChunkUsedSize, NodeList),
    
    
    {reply, Reply, State};

handle_call({locatechunk, FileID, ChunkIndex}, From, State) ->
    Reply = do_get_chunk(FileID, ChunkIndex),
    {reply, Reply, State};

handle_call({close, FileID}, {From, _}, State)->
	Reply = do_close(FileID, From),
    {reply, Reply, State};

handle_call(_, {From, _}, State)->
	Reply = {error, "undefined handler"},
    {reply, Reply, State}.

handle_cast(stop, State) ->
    io:format("meta server stopping~n"),
    {stop, normal, State}.

handle_info(Info, State) ->
    {noreply, State}.

%"client api" methods
% write operation sequence:
% 1. open(client) =>
% 2. writeAllocate(client) =>
% 3. registerChunk(chunkserver) =>
% 4. close(client)

% read operation sequence:
% 1. open(client) =>
% 2. locateChunk(client) =>
% 3. close(client)

open(FileName,Mode) ->
    gen_server:call(?GM, {open, FileName,Mode}).

open2(FileName,Mode) ->
    gen_server:call(?GM,{open2,FileName,Mode}).

writeAllocate(FileID) ->
    gen_server:call(?GM, {allocatechunk, FileID}).

locateChunk(FileID, ChunkIndex)->
    gen_server:call(?GM, {locatechunk, FileID, ChunkIndex}).

registerChunk(FileID, ChunkID, ChunkUsedSize, NodeList)->
    gen_server:call(?GM, {registerchunk, FileID, ChunkID, ChunkUsedSize, NodeList}).

close(FileID)->
    gen_server:call(?GM, {close, FileID}).

