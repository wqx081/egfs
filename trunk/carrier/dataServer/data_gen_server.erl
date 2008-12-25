-module(data_gen_server).
-behaviour(gen_server).
-import(data_worker, [handle_read/3, handle_write/4]).
-include("../include/egfs.hrl").
-export([start/0, stop/0, 
	 init/1, handle_call/3,
	 handle_cast/2, handle_info/2, 
	 terminate/2, code_change/3]).

%% -record(chunk_info, {chunkID, location, fileID, index}).
%% -define(TABLE, "chunk_table").
-define(DATA_SERVER, {global, data_server}).

start() -> 
    gen_server:start_link(?DATA_SERVER, ?MODULE, [], []).

stop() ->
    gen_server:cast(?DATA_SERVER, stop).

init([]) -> 
    process_flag(trap_exit, true),
    ?DEBUG("data_gen_server is starting ~n", []),
    %% {ok, ?TABLE} = dets:open_file(?TABLE, [{file, ?TABLE}]),
    {ok, server_has_startup}.

handle_call({readchunk, ChkID, Begin, Size}, _From, N) ->
    ?DEBUG("[data_server]: read request from client, chunkID(~p)~n", [ChkID]),
    Reply = handle_read(ChkID, Begin, Size),
    {reply, Reply, N};
handle_call({writechunk, FileID, ChunkIndex, ChunkID, _Nodelist}, _From, N) ->
    ?DEBUG("[data_server]: write request from client, chkID(~p)~n", [ChunkID]),
    Reply = handle_write(FileID, ChunkIndex, ChunkID, _Nodelist),
    {reply, Reply, N};
handle_call({echo, Msg}, _From, N) ->
    ?DEBUG("[data_server]: echo ~p~n", [Msg]),
    {reply, Msg, N};
handle_call(Any, _From, N) ->
    ?DEBUG("[data_server]: unknown request ~p~n", [Any]),
    {noreply, N}.
    
handle_cast(_Msg, N) -> {noreply, N}.
handle_info(_Info, N) -> {noreply, N}.

terminate(_Reason, _N) ->
    ?DEBUG("~p is stopping~n", [?MODULE]),
    %% dets:close(?TABLE),
    ok.

code_change(_OldVsn, N, _Extra) -> {ok, N}.
