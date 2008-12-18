%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : 
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(client).
-define(DataServer,"192.168.0.111").
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

write(FileName, Amount) -> gen_server:call(?MODULE, {write, FileName, Amount}).

read(FileName)      -> gen_server:call(?MODULE, {read, FileName}).

delete(FileName)    -> gen_server:call(?MODULE, {del, FileName}).

close(FileName)     -> gen_server:call(?MODULE, {close, FileName}).

init([]) -> {ok, {}}.

%%----------------------------------------------------------------------
%% handle_call functions

%%----------------------------------------------------------------------
handle_call({open,FileName,X}, _From, Tab) ->
    %%TODO: add "send the open_info to metaserver" code here
    Reply = case X of
		r  -> {you_have_openned_a_file, FileName, X};
		w  -> {you_have_openned_a_file, FileName, X}
	    end,
    {reply, Reply, Tab};
handle_call({write,FileName,X}, _From, Tab) ->
    %%TODO: add "send the write_info to metaserver" code here
    writechunk(),
    Reply = {thanks, FileName, you_write_to_the_file, X},
    {reply, Reply, Tab};
handle_call({read,FileName}, _From, Tab) ->
    %%TODO: add "send the read_info to metaserver" code here
    readchunk(2000, {0, 1024}), 
    Reply ={you_have_read_a_file, FileName, 2000},
    {reply, Reply, Tab};
handle_call({del,FileName}, _From, Tab) ->
    %%TODO: add "send the del_info to metaserver" code here
    Reply ={you_have_read_a_file, FileName, 2000},
    {reply, Reply, Tab};
handle_call({close,_FileName}, _From, Tab) ->
    {stop, normal, closed, Tab}.

handle_cast(_Msg, State)    -> {noreply, State}.
handle_info(_Info, State)   -> {noreply, State}.
terminate(_Reason, _State)  -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%----------------------------------------------------------------------
%% read chunk_data form dataserver
%%----------------------------------------------------------------------
readchunk(FileID,{Start_addr, End_addr}) ->
    {ok, Socket} = gen_tcp:connect(?DataServer, 9999, [binary, {packet, 2}, {active, true}]),
    Read_req = {read, FileID, Start_addr, End_addr},%%2000, 0, 1024 * 1024 * 1024
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

test_write() ->
    {ok, Hdl} = file:open("send.dat", [raw, read, binary]),
    {ok, Binary} = file:pread(Hdl, 5, 4),
    {ok, Hdlw} = file:open("recv.dat", [raw, append, binary]),
    write(Binary, Hdlw).


%%----------------------------------------------------------------------
%% write chunk_data to dataserver

%%----------------------------------------------------------------------
writechunk() ->
    {ok, Socket} = gen_tcp:connect(?LocalServer, 9999, [binary, {packet, 2}, {active, true}]),
    Write_req = {write, 2000},%%2000, 0, 1024 * 1024 * 1024
    ok = gen_tcp:send(Socket, term_to_binary(Write_req)),
    process_flag(trap_exit, true),      %%do what?
    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),

	    case Response of
		{ok, Port} ->
		    _Child = spawn_link(fun() -> send_data(?LocalServer, Port) end);
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
    

