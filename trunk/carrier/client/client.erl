%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : 
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(client).
-include_lib("kernel/include/file.hrl").
%% display debug info or not
-define(debug,"YES").
-ifdef(debug).
-define(DEBUG(Fmt, Args), io:format(Fmt, Args)).
-else.
-define(DEBUG(Fmt, Args), no_debug).
-endif.
%-define(DataServer,"192.168.0.111").
-define(ChunkSize, 64*1024*1024).
-define(STRIP_SIZE, 8192).
-export([init/1, write/1, code_change/3]).
-compile(export_all).

%%----------------------------------------------------------------------
%% client_api,can be used for application progamming
%%----------------------------------------------------------------------
open(FileName, Mode) ->  case   gen_server:call({global,metaserver}, {open, FileName, Mode}) of
                                {ok, FileID} -> 
                                    ?DEBUG("Open file ok!FileName is:~p and FileID is:~p~n",[FileName,FileID]),
                                    {ok, FileID};
                                {error, _Why} -> ?DEBUG("Open file error~n",[])
                        end.

write(FileName) ->
    {ok, FileID} = open(FileName, w),
    ?DEBUG("Open file ok!----FileName is:~p and FileID is:~p~n",[FileName,FileID]),
    writechunks(FileName,FileID).

read(FileName,{Start_addr, End_addr}) ->
    {ok, FileID} = open(FileName,r),
    readchunks(FileID,{Start_addr, End_addr}).

delete(FileName)    -> case     gen_server:call({global,metaserver}, {delete, FileName})    of
                                {ok} -> ?DEBUG("Delete file ok!~n",[]);
                                {error, _Why} -> ?DEBUG("Delete file error~n",[])
                        end.

close(FileName)     -> case     gen_server:call({global,metaserver}, {close, FileName})     of
                                {ok} -> ?DEBUG("Close file ok!~n",[]);
                                {error, _Why} -> ?DEBUG("Close file error~n",[])
                        end.
init([]) -> {ok, {}}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
%%----------------------------------------------------------------------
%% read chunk_data form dataserver
%%----------------------------------------------------------------------
readchunks(FileID,{Start_addr, End_addr}) ->
    Start_ChunkIndex = Start_addr div ?ChunkSize,
    End_ChunkIndex = Start_addr div ?ChunkSize,
    Start = Start_addr rem ?ChunkSize,
    End = End_addr rem ?ChunkSize,
    loop_read_chunk(FileID, Start_ChunkIndex, End_ChunkIndex, Start, End).
    
loop_read_chunk(FileID, ChunkIndex, End_ChunkIndex, Start, End) when End_ChunkIndex >=ChunkIndex ->    
    case  End_ChunkIndex-ChunkIndex  of
        0  ->
            Size = End - Start;
        true ->
            Size = ?ChunkSize - Start
    end,
    {ok, ChunkID,NodeList} = gen_server:call({global,metaserver}, {locatechunk, FileID, ChunkIndex}),
    _NodeID=hd(NodeList),
    %{ok,DataServer}=inet:getaddr(NodeID,inet4),
    Result = gen_server:call({global,data_server}, {readchunk, ChunkID, Start, Size}),%%lt中为read，应改为readchunk
    {ok, Host, Port} = Result,
    ?DEBUG("Result ~p~n", [Result]),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    process_flag(trap_exit, true),
    receive
        {tcp, Socket, Binary} ->
            {ok, Data_Port} = binary_to_term(Binary),
            %% spawn_link(fun() -> receive_data(Host, Data_Port) end);
            receive_data(Host, Data_Port),
            Index1 = ChunkIndex + 1,
            loop_read_chunk(FileID, Index1, End_ChunkIndex, 0, End);
        {tcp_close, Socket} ->
            ?DEBUG("read file closed~n",[]),
            void
    end.

    
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
    file:write(Hdl, Data).
    %% file:close(Hdl).


%%----------------------------------------------------------------------
%% write chunk_data to dataserver
%%----------------------------------------------------------------------
writechunks(FileName, FileID)->
    {ok, ChunkID,NodeList} = gen_server:call({global,metaserver}, {allocatechunk, FileID}),
    ?DEBUG("you have get them,ChunkID:~p and NodeList:~p~n",[ChunkID,NodeList]),
    Result = gen_server:call({global,data_server}, {writechunk, FileID, ChunkID, NodeList}),
    {ok, Host, Port} = Result,
    _NodeID=hd(NodeList),
    %{ok,DataServer}=inet:getaddr(NodeID,inet4),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
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
    %process_flag(trap_exit, true),把该进程当成系统进程，为了子进程退出时，父进程可以收到推出消息。


send_data(Host, Port, FileName) ->
    {ok, Hdl} = file:open(FileName, [raw, read, binary]),
    {ok, FileLength} = get_file_size(FileName),
    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    ?DEBUG("Transfer data begin: ~p~n", [erlang:time()]),
    loop_send(DataSocket, Hdl, 0, FileLength),
    ?DEBUG("Transfer data end: ~p~n", [erlang:time()]).    


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