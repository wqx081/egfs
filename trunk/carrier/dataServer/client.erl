-module(client).
-include_lib("kernel/include/file.hrl").
-include("../include/egfs.hrl").
-export([test_r/0, test_w/0, test_naive/0, test_write/0]).
-define(HOST, "192.168.0.111").
-define(STRIP_SIZE, 8192).

test_r() ->
    %%{ok, Host, Port} = gen_server:call({global, data_server}, {read, 2000, 0, 1024*1024*256}).
    Result = gen_server:call({global, data_server}, {read, 2000, 0, 268435456}),
    io:format("Result ~p~n", [Result]),
    %% ok, Host, Port} = global:send(data_server, {read, 2000, 0, 1024*1024*256}),
    %% ok, Host, Port} = data_server ! {read, 2000, 0, 1024*1024*256},
    {ok, Host, Port} = Result,
    receive_control(Host, Port).

test_w() ->
    Result = gen_server:call({global, data_server}, {write, {chunkID, 2000}}),
    io:format("Result ~p~n", [Result]),
    {ok, Host, Port} = Result,
    send_control(Host, Port).

test_naive() ->
    {ok, Socket} = gen_tcp:connect(?HOST, 9999, [binary, {packet, 2}, {active, true}]),
    Read_req = {read, 2000, 0, 1024 * 1024 *256},
    ok = gen_tcp:send(Socket, term_to_binary(Read_req)),

    process_flag(trap_exit, true),

    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),

	    case Response of
		{ok, Port} ->
		    _Child = spawn_link(fun() -> receive_data(?HOST, Port) end);
		{error, _Why} ->
		    io:format("Read Req can't be satisfied~n")
	    end
    end.

%% send data to data server
send_control(Host, Port) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Binary} ->
	    {ok, Data_Port} = binary_to_term(Binary),
	    %% spawn_link(fun() -> receive_data(Host, Data_Port) end);
	    send_data(Host, Data_Port);
	{tcp_close, Socket} ->
	    void
    end.

send_data(Host, Port) ->
    {ok, Hdl} = file:open("send.dat", [binary, raw, read]),
    {ok, FileSize} = get_file_size("send.dat"),

    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    io:format("Transfer begin: ~p~n", [erlang:time()]),
    loop_send(DataSocket, Hdl, 0, FileSize),
    io:format("Transfer end: ~p~n", [erlang:time()]).

loop_send(DataSocket, Hdl, Begin, End) when Begin < End ->
    {ok, Binary} = file:pread(Hdl, Begin, ?STRIP_SIZE),
    gen_tcp:send(DataSocket, Binary),
    Begin1 = Begin + ?STRIP_SIZE,
    loop_send(DataSocket, Hdl, Begin1, End);
loop_send(_, _, _, _) ->
    void.

    
%% receive data from data server
receive_control(Host, Port) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Binary} ->
	    {ok, Data_Port} = binary_to_term(Binary),
	    %% spawn_link(fun() -> receive_data(Host, Data_Port) end);
	    receive_data(Host, Data_Port);
	{tcp_close, Socket} ->
	    void
    end.

receive_data(Host, Port) ->
    io:format("data port:~p~n", [Port]),
    {ok, Hdl} = file:open("recv.dat", [raw, append, binary]),

    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    io:format("Transfer begin: ~p~n", [erlang:time()]),
    loop(DataSocket, Hdl),
    io:format("Transfer end: ~p~n", [erlang:time()]).

loop(DataSocket, Hdl) ->
    receive
	{tcp, DataSocket, Data} ->
	    write(Data, Hdl),
	    loop(DataSocket, Hdl);
	{tcp_closed, DataSocket} ->
	    io:format("read chunk over!~n");
	{client_close, _Why} ->
	    io:format("client close the datasocket~n"),
	    gen_tcp:close(DataSocket)
    end.

write(Data, Hdl) ->
    %% {ok, Hdl} = file:open("recv.dat", [raw, append, binary]),
    file:write(Hdl, Data).
    %% file:close(Hdl).

test_write() ->
    {ok, Hdl} = file:open("send.dat", [raw, read, binary]),
    {ok, Binary} = file:pread(Hdl, 0, 256),
    {ok, Hdlw} = file:open("recv.dat", [raw, append, binary]),
    file:truncate(Hdlw),
    write(Binary, Hdlw),
    write(Binary, Hdlw).

get_file_size(FileName) ->
    case file:read_file_info(FileName) of
	{ok, Facts} ->
	    {ok, Facts#file_info.size};
	_ ->
	    error
    end.
