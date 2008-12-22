%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : 
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(client).
-define(MetaServer,"192.168.0.102").
-define(DataServer,"192.168.0.102").
-define(LocalServer,"192.168.0.102").

-behaviour(gen_server).
-export([start/0]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-compile(export_all).

%%----------------------------------------------------------------------
%% client_api,can be used for application progamming

%%----------------------------------------------------------------------
start() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

open(FileName,Mode) -> gen_server:call(?MODULE, {open, FileName,Mode}).

write(FileName) -> gen_server:call(?MODULE, {write, FileName}).

read(FileName,{Start_addr, End_addr})-> gen_server:call(?MODULE, {read,FileName,{Start_addr, End_addr}}).

delete(FileName)    -> gen_server:call(?MODULE, {del, FileName}).

close(FileName)     -> gen_server:call(?MODULE, {close, FileName}).

init([]) -> {ok, {}}.

%%----------------------------------------------------------------------
%% handle_call functions
%%----------------------------------------------------------------------
handle_call({open,FileName,Mode}, _From, Tab) ->
    send_open(FileName, Mode),
    Reply = {you_have_openned_a_file, FileName, Mode},
    {reply, Reply, Tab};
handle_call({write,FileName}, _From, Tab) ->
    {ok, _Port, FileID} = send_open(FileName, w),
    %-------------------这一段的内容要循环做,用递归吗----------------------
    writechunks(FileID),
    send_close(FileName),
    %-------------------------------------------------------------
    Reply = {thanks, FileName, you_write_to_the_file},
    {reply, Reply, Tab};
handle_call({read,FileName,{Start_addr, End_addr}}, _From, Tab) ->
    {ok, _Port, FileID} = send_open(FileName, r),  
    ChunkIndex = Start_addr/64,
    readchunks(FileID, ChunkIndex,{Start_addr, End_addr}),
    send_close(FileName),
    Reply ={you_have_read_a_file, FileName,{Start_addr, End_addr}},
    {reply, Reply, Tab};
handle_call({del, FileName}, _From, Tab) ->
    send_delete(FileName),
    Reply ={you_have_read_a_file, FileName},
    {reply, Reply, Tab};
handle_call({close, FileName}, _From, Tab) ->
    send_close(FileName),
    {stop, normal, closed, Tab}.

handle_cast(_Msg, State)    -> {noreply, State}.
handle_info(_Info, State)   -> {noreply, State}.
terminate(_Reason, _State)  -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.




send_open(FileName, Mode)->
    {ok, Socket} = gen_tcp:connect(?MetaServer, 9998, [binary, {packet, 2}, {active, true}]),
    Open_req = {open, FileName, Mode},%%2000, 0, 1024 * 1024 * 1024
    ok = gen_tcp:send(Socket, term_to_binary(Open_req)),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),
	    case Response of
		{ok, _Port, FileID} ->
                    io:format("Open Req ok:~p~n",[FileID]);
		{error, _Why} ->
		    io:format("Write Req can't be satisfied~n")
	    end
    end.



send_close(FileName)->
    {ok, Socket} = gen_tcp:connect(?MetaServer, 9998, [binary, {packet, 2}, {active, true}]),
    Close_req = {close, FileName},%%2000, 0, 1024 * 1024 * 1024
    ok = gen_tcp:send(Socket, term_to_binary(Close_req)),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),
	    case Response of
		{ok, _Port, FileID} ->
                    io:format("Close Req ok:~p~n",[FileID]);
		{error, _Why} ->
		    io:format("Close Req can't be satisfied~n")
	    end
    end.


send_delete(FileName)->
    {ok, Socket} = gen_tcp:connect(?MetaServer, 9998, [binary, {packet, 2}, {active, true}]),
    Del_req = {del, FileName},%%2000, 0, 1024 * 1024 * 1024
    ok = gen_tcp:send(Socket, term_to_binary(Del_req)),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),
	    case Response of
		{ok, _Port, FileID} ->
                    io:format("Del Req ok:~p~n",[FileID]);
		{error, _Why} ->
		    io:format("Del Req can't be satisfied~n")
	    end
    end.

%%----------------------------------------------------------------------
%% read chunk_data form dataserver
%%----------------------------------------------------------------------
readchunks(FileID, ChunkIndex,{Start_addr, End_addr}) ->
    {ok, _Port, ChunkID, NodeList} = locatechunk(FileID, ChunkIndex),
    %{H|T}=NodeList,
    readchunk(ChunkID, hd(NodeList), {Start_addr, End_addr}),
    readchunks(FileID, ChunkIndex,{Start_addr, End_addr}).
    


locatechunk(FileID, ChunkIndex)->
    {ok, Socket} = gen_tcp:connect(?MetaServer, 9998, [binary, {packet, 2}, {active, true}]),
    Locate_req = {locate, FileID, ChunkIndex},%%2000, 0, 1024 * 1024 * 1024
    ok = gen_tcp:send(Socket, term_to_binary(Locate_req)),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),
	    case Response of
		{ok, _Port, ChunkID, _NodeList} ->
                    io:format("locate Req ok:~p~n",[ChunkID]);
		{error, _Why} ->
		    io:format("locate Req can't be satisfied~n")
	    end
    end.

readchunk(ChunkID, _node, {Start_addr, End_addr}) ->
    {ok, Socket} = gen_tcp:connect(?DataServer, 9999, [binary, {packet, 2}, {active, true}]),
    Read_req = {read, ChunkID, Start_addr, End_addr},
    ok = gen_tcp:send(Socket, term_to_binary(Read_req)),
    process_flag(trap_exit, true),
    receive                         %%receive info from socket of dataserver
	{tcp, Socket, Bin} ->               
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),
	    case Response of
		{ok, Port} ->
		    _Child = spawn_link(fun() -> receive_data(?DataServer, Port) end);
		{error, _Why} ->
		    io:format("Read Req can't be satisfied~n")
	    end
    end.
    
receive_data(Host, Port) ->
    {ok, Hdl} = file:open("recv.dat", [raw, append, binary]),
    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    io:format("Receive data begin: ~p~n", [erlang:time()]),
    loop_recv(DataSocket, Hdl),
    io:format("Receive data end: ~p~n", [erlang:time()]).

loop_recv(DataSocket, Hdl) ->
    receive
	{tcp, DataSocket, Data} ->
	    write_data(Data, Hdl),
	    loop_recv(DataSocket, Hdl);
	{tcp_closed, DataSocket} ->
	    io:format("read chunk over!~n");
	{client_close, _Why} ->
	    io:format("client close the datasocket~n"),
	    gen_tcp:close(DataSocket)
    end.

write_data(Data, Hdl) ->
    %% {ok, Hdl} = file:open("recv.dat", [raw, append, binary]),
    file:write(Hdl, Data).
    %% file:close(Hdl).

%test_write() ->
%    {ok, Hdl} = file:open("send.dat", [raw, read, binary]),
%    {ok, Binary} = file:pread(Hdl, 5, 4),
%    {ok, Hdlw} = file:open("recv.dat", [raw, append, binary]),
%    write(Binary, Hdlw).


%%----------------------------------------------------------------------
%% write chunk_data to dataserver

%%----------------------------------------------------------------------
writechunks(FileID)->
    {ok, _Port, ChunkID, NodeList} = allocatechunk(FileID),
    writechunk(FileID,ChunkID,NodeList),
    writechunks(FileID).



allocatechunk(FileID)->
    {ok, Socket} = gen_tcp:connect(?MetaServer, 9998, [binary, {packet, 2}, {active, true}]),
    Allocate_req = {allocate, FileID},
    ok = gen_tcp:send(Socket, term_to_binary(Allocate_req)),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),
	    case Response of
		{ok, _Port, ChunkID, _NodeList} ->
                    io:format("allocate Req ok:~p~n",[ChunkID]);
		{error, _Why} ->
		    io:format("allocate Req can't be satisfied~n")
	    end
    end.


writechunk(FileID,ChunkID,_NodeList) ->
    {ok, Socket} = gen_tcp:connect(?DataServer, 9999, [binary, {packet, 2}, {active, true}]),
    Write_req = {write, FileID, ChunkID},
    ok = gen_tcp:send(Socket, term_to_binary(Write_req)),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),
	    case Response of
		{ok, Port} ->
		    _Child = spawn_link(fun() -> send_data(?DataServer, Port) end);
		{error, _Why} ->
		    io:format("Write Req can't be satisfied~n")
	    end
    end.
    
send_data(Host, Port) ->
    {ok, Hdl} = file:open("recv.dat", [raw, read, binary]),
    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    io:format("Transfer data begin: ~p~n", [erlang:time()]),
    loop_send(DataSocket, Hdl),
    io:format("Transfer data end: ~p~n", [erlang:time()]).    


loop_send(DataSocket, Hdl) ->
    receive
	{tcp, DataSocket, _Data} ->
	    read_data(DataSocket, Hdl, 0, 1024),
            %%write_data(Data, Hdl),
	    loop_send(DataSocket, Hdl);
	{tcp_closed, DataSocket} ->
	    io:format("write chunk over!~n");
	{client_close, _Why} ->
	    io:format("client close the datasocket~n"),
	    gen_tcp:close(DataSocket)
    end.

read_data(DataSocket, Hdl, Begin, Size) ->
    {ok, Binary} = file:pread(Hdl, Begin, Size),
    gen_tcp:send(DataSocket, Binary).


%----------------------------------------------------------

