%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : 
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(client).
-include_lib("kernel/include/file.hrl").
-include("../include/egfs.hrl").
-define(ChunkSize, 1024).%64*1024*
-define(STRIP_SIZE, 8192).


%-behaviour(gen_server).
-export([start/0]).
%-define(DataServer,"192.168.0.111").
-export([init/1, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-compile(export_all).

%%----------------------------------------------------------------------
%% client_api,can be used for application progamming
%%----------------------------------------------------------------------
start() -> gen_server:start_link({global,?MODULE}, ?MODULE, [], []).

open(FileName, Mode) ->
    case    gen_server:call({global,metaserver}, {open, FileName, Mode}) of
        {ok, FileID} ->
            ?DEBUG("Open file ok!FileName is:~p and FileID is:~p~n",[FileName,FileID]),
            {ok, FileID};
        {error, Why} -> ?DEBUG("Open file error~p~n",[Why])
    end.

write(FileName) ->
    {ok, FileID} = open(FileName, w),
    ?DEBUG("Open file ok!----FileName is:~p and FileID is:~p~n",[FileName,FileID]),
    writechunks(FileName,FileID),
    close(FileID).

read(FileName,{Start_addr, End_addr}) ->
    {ok, FileID} = open(FileName,r),
    ?DEBUG("Start_addr,End_addr~p~p~n", [Start_addr,End_addr]),
    readchunks(FileID,{Start_addr, End_addr}),
    close(FileID).

delete(FileName)    -> 
    case    gen_server:call({global,metaserver}, {delete, FileName})    of
        {ok} -> ?DEBUG("Delete file ok!~n",[]);
        {error, Why} -> ?DEBUG("Delete file error~p~n",[Why])
    end.

close(FileID)     ->
    case     gen_server:call({global,metaserver}, {close, FileID})    of
        { ok } -> ?DEBUG("Close file ok!~n",[]);
        {error, Why} -> ?DEBUG("Close file error~p~n",[Why])
    end.
init([]) -> {ok, {}}.

handle_cast(_Msg, State)    -> {noreply, State}.
handle_info(_Info, State)   -> {noreply, State}.
terminate(_Reason, _State)  ->
    unregister({global,?MODULE}),
    ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
%%----------------------------------------------------------------------
%% read chunk_data form dataserver
%%----------------------------------------------------------------------
readchunks(FileID,{Start_addr, End_addr}) ->
    Start_ChunkIndex = Start_addr div ?ChunkSize,
    End_ChunkIndex = End_addr div ?ChunkSize,
    Start = Start_addr rem ?ChunkSize,
    End = End_addr rem ?ChunkSize,
    ?DEBUG("Start_ChunkIndex,End_ChunkIndex,Start,End ~p-~p-~p-~p~n", [Start_ChunkIndex,End_ChunkIndex,Start,End]),
    loop_read_chunk(FileID, Start_ChunkIndex, End_ChunkIndex, Start, End).
    
loop_read_chunk(FileID, ChunkIndex, End_ChunkIndex, Start, End) when End_ChunkIndex >=ChunkIndex ->    
    case  End_ChunkIndex-ChunkIndex  of
        0  ->
            Size = End - Start;
        _Any ->
            Size = ?ChunkSize - Start
    end,
    ?DEBUG("Size is ok:~p~n", [Size]),
    ?DEBUG("ChunkIndex,End_ChunkIndex,Start,End ~p-~p-~p-~p~n", [ChunkIndex,End_ChunkIndex,Start,End]),
    {ok, ChunkID,NodeList} = gen_server:call({global,metaserver}, {locatechunk, FileID, ChunkIndex}),
    _NodeID=hd(NodeList),
    %{ok,DataServer}=inet:getaddr(NodeID,inet4),
   
    Result = gen_server:call({global,data_server}, {readchunk, ChunkID, Start, Size}),
    {ok, Host, Port} = Result,
    ?DEBUG("{ok, Host, Port} is: ~p~n", [Result]),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    ?DEBUG("you have get them,Socket is:~p ~n",[Socket]),
    process_flag(trap_exit, true),
    receive
        {tcp, Socket, Binary} ->
            {ok, Data_Port} = binary_to_term(Binary),
            %% spawn_link(fun() -> receive_data(Host, Data_Port) end);
            receive_data(Host, Data_Port),
            Index1 = ChunkIndex + 1,
            ?DEBUG("FileID, Index1, End_ChunkIndex, 0, End is: ~p--~p--~p--~p~n", [FileID, Index1, End_ChunkIndex, End]),
            loop_read_chunk(FileID, Index1, End_ChunkIndex, 0, End);

        {tcp_close, Socket} ->
            ?DEBUG("read file closed~n",[]),
            void
    end;
loop_read_chunk(_, _, _, _, _) ->
    ?DEBUG("read file closed~n",[]),
    void.

    
receive_data(Host, Port) ->
    ?DEBUG("data port:~p~n", [Port]),
    {ok, Hdl} = file:open("recv.dat", [raw, append, binary]),
    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    ?DEBUG("Receive data begin: ~p~n", [erlang:time()]),
    loop_recv(DataSocket, Hdl),
    ?DEBUG("Receive data end: ~p~n", [erlang:time()]).

loop_recv(DataSocket, Hdl) ->
    receive
	{tcp, DataSocket, Data} ->
	    write_data(Data, Hdl),
	    loop_recv(DataSocket, Hdl);
	{tcp_closed, DataSocket} ->
	    ?DEBUG("read chunk over!~n",[]);
	{client_close, _Why} ->
	    ?DEBUG("client close the datasocket~n",[]),
	    gen_tcp:close(DataSocket)
    end.

write_data(Data, Hdl) ->
    %% {ok, Hdl} = file:open("recv.dat", [raw, append, binary]),
    ?DEBUG("write data now!~n",[]),
    file:write(Hdl, Data).
    %% file:close(Hdl).


%%----------------------------------------------------------------------
%% write chunk_data to dataserver
%%----------------------------------------------------------------------
writechunks(FileName, FileID)->
    {ok, ChunkID,NodeList} = gen_server:call({global,metaserver}, {allocatechunk, FileID}),
    ?DEBUG("you have get them,ChunkID:~p and NodeList:~p~n",[ChunkID,NodeList]),
    ChunkIndex = 0,
    Result = gen_server:call({global,data_server}, {writechunk, FileID, ChunkIndex, ChunkID, NodeList}),    
    {ok, Host, Port} = Result,
    ?DEBUG("you have get them,Host:~p and Port:~p~n",[Host, Port]),
    _NodeID = hd(NodeList),
    %{ok,DataServer}=inet:getaddr(NodeID,inet4),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    ?DEBUG("you have get them,Socket is:~p ~n",[Socket]),
	process_flag(trap_exit, true),
    receive
        {tcp, Socket, Binary} ->
            {ok, Data_Port} = binary_to_term(Binary),
            %% spawn_link(fun() -> receive_data(Host, Data_Port) end);
            send_data(Host, Data_Port, FileName);
        {tcp_close, Socket} ->
            ?DEBUG("write file closed~n",[]),
            void
    end.


send_data(Host, Port, FileName) ->
    {ok, Hdl} = file:open(FileName, [raw, read, binary]),
    {ok, FileLength} = get_file_size(FileName),
    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    ?DEBUG("Transfer data begin: ~p~n", [erlang:time()]),
    loop_send(DataSocket, Hdl, 0, FileLength),
    gen_tcp:close(DataSocket),
    ?DEBUG("Transfer data end: ~p~n", [erlang:time()]),
    file:close(Hdl).


loop_send(DataSocket, Hdl, Begin, End) when Begin < End ->
    {ok, Binary} = file:pread(Hdl, Begin, ?STRIP_SIZE),
    gen_tcp:send(DataSocket, Binary),
    Begin1 = Begin + ?STRIP_SIZE,
    loop_send(DataSocket, Hdl, Begin1, End);

loop_send(_, _, _, _) ->
    void.

get_file_size(FileName) ->
    case file:read_file_info(FileName) of
	{ok, Facts} ->
	    {ok, Facts#file_info.size};
	_ ->
	    error
    end.