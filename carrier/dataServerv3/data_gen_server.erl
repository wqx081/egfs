-module(data_gen_server).
-behaviour(gen_server).
%% -import(data_worker, [handle_read/3, handle_write/4]).
-import(data_writer, [handle_write/6]).
-import(data_reader, [handle_read/4]).
-import(chunk_db).
-include("../include/egfs.hrl").
-include("data_server.hrl").
-export([start/0, stop/0, 
	 init/1, handle_call/3,
	 handle_cast/2, handle_info/2, 
	 terminate/2, code_change/3]).

-define(DATA_SERVER, {local, ?SERVER_NAME}).

start() -> 
    gen_server:start_link(?DATA_SERVER, ?MODULE, [], []).

stop() ->
    gen_server:cast(?DATA_SERVER, stop).

init([]) -> 
    chunk_db:start(),
    %% boot_report:boot_report(),
    %% heart_beat_report:start(),
    %% chunk_garbage_collect:start_auto_collect(),
    Result = toolkit:start_listen(),
    Result.

handle_call({readchunk, ChkID, Begin, Size}, _From, Inet) ->
    ?DEBUG("[data_server]: read request from client, chunkID(~p)~n", [ChkID]),
    Reply = handle_read(ChkID, Begin, Size, Inet),
    {reply, Reply, Inet};
handle_call({writechunk, FileID, ChunkID, Begin, Size, Nodelist}, _From, Inet) ->
    ?DEBUG("[data_server]: write request from client, chkID(~p)~n", [ChunkID]),
    Reply = handle_write(FileID, ChunkID, Begin, Size, Nodelist, Inet),
    {reply, Reply, Inet};
handle_call({echo, Msg}, _From, N) ->
    ?DEBUG("[data_server]: echo ~p~n", [Msg]),
    io:format("[~p, ~p] ~p~n", [?MODULE, ?LINE, _From]),
    {reply, Msg, N}.
%%handle_call(Any, _From, N) ->
%%    ?DEBUG("[data_server]: unknown request ~p~n", [Any]),
%%    {noreply, N}.
    
handle_cast(_Msg, N) -> {noreply, N}.
handle_info(_Info, N) -> {noreply, N}.

terminate(_Reason, _N) ->
    ?DEBUG("~p is stopping~n", [?MODULE]),
    chunk_db:stop(),
    ok.

code_change(_OldVsn, N, _Extra) -> {ok, N}.

