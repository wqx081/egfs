%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : 
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(client).
%% display debug info or not
%%-define(debug,"YES").
-ifdef(debug).
-define(DEBUG(Fmt, Args), io:format(Fmt, Args)).
-else.
-define(DEBUG(Fmt, Args), no_debug).
-endif.
-define(DataServer,"192.168.0.102").
-export([init/1, code_change/3]).
-compile(export_all).

%%----------------------------------------------------------------------
%% client_api,can be used for application progamming
%%----------------------------------------------------------------------
open(FileName,Mode) ->  case    gen_server:call({global,metaserver}, {open, FileName,Mode}) of
                                {ok, _FileID} -> ?DEBUG("Open file ok!~n",[]);
                                {error, _Why} -> ?DEBUG("Open file error~n",[])
                        end.

write(FileName) ->
    {ok, FileID} = open(FileName,w),
    {ok, ChunkID,NodeList} = gen_server:call({global,metaserver}, {allocatechunk, FileID}),
    {ok, _State} = gen_server:call({global,dataserver}, {writechunk, FileID, ChunkID, NodeList}),
    writechunk(FileID,ChunkID,NodeList).

read(FileName,{Start_addr, End_addr}) ->
    {ok, FileID} = open(FileName,r),
    ChunkIndex = Start_addr/64,
    {ok, ChunkID,NodeList} = gen_server:call({global,metaserver}, {locatechunk, FileID, ChunkIndex}),
    NodeID=hd(NodeList),
    gen_server:call({global,dataserver}, {readchunk, ChunkID, NodeID,{Start_addr, End_addr}}),
    readchunk(ChunkID, NodeID, {Start_addr, End_addr}).

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
readchunk(ChunkID, _node, {Start_addr, End_addr}) ->
    {ok, Socket} = gen_tcp:connect(?DataServer, 9999, [binary, {packet, 2}, {active, true}]),
    Read_req = {read, ChunkID, Start_addr, End_addr},
    ok = gen_tcp:send(Socket, term_to_binary(Read_req)),
    process_flag(trap_exit, true),
    receive                         %%receive info from socket of dataserver
	{tcp, Socket, Bin} ->               
	    Response = binary_to_term(Bin),
	    ?DEBUG("Response:~p~n", [Response]),
	    case Response of
		{ok, Port} ->
		    _Child = spawn_link(fun() -> receive_data(?DataServer, Port) end);
		{error, _Why} ->
		    ?DEBUG("Read Req can't be satisfied~n",[])
	    end
    end.
    
receive_data(Host, Port) ->
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


writechunk(FileID,ChunkID,_NodeList) ->
    {ok, Socket} = gen_tcp:connect(?DataServer, 9999, [binary, {packet, 2}, {active, true}]),
    Write_req = {write, FileID, ChunkID},
    ok = gen_tcp:send(Socket, term_to_binary(Write_req)),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    ?DEBUG("Response:~p~n", [Response]),
	    case Response of
		{ok, Port} ->
		    _Child = spawn_link(fun() -> send_data(?DataServer, Port) end);
		{error, _Why} ->
		    ?DEBUG("Write Req can't be satisfied~n",[])
	    end
    end.
    
send_data(Host, Port) ->
    {ok, Hdl} = file:open("recv.dat", [raw, read, binary]),
    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    ?DEBUG("Transfer data begin: ~p~n", [erlang:time()]),
    loop_send(DataSocket, Hdl),
    ?DEBUG("Transfer data end: ~p~n", [erlang:time()]).    


loop_send(DataSocket, Hdl) ->
    receive
	{tcp, DataSocket, _Data} ->
	    read_data(DataSocket, Hdl, 0, 1024),
            %%write_data(Data, Hdl),
	    loop_send(DataSocket, Hdl);
	{tcp_closed, DataSocket} ->
	    ?DEBUG("write chunk over!~n",[]);
	{client_close, _Why} ->
	    ?DEBUG("client close the datasocket~n",[]),
	    gen_tcp:close(DataSocket)
    end.

read_data(DataSocket, Hdl, Begin, Size) ->
    {ok, Binary} = file:pread(Hdl, Begin, Size),
    gen_tcp:send(DataSocket, Binary).

