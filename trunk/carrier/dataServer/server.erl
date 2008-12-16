-module(server).
-export([start/0]).

start() ->
    {ok, Listen} = gen_tcp:listen(9999, [binary, 
					 {packet, 2}, 
					 {active, true},
					 {reuseaddr, true}]),
    register(?MODULE, self()),
    spawn(fun() -> par_connect(Listen) end).

par_connect(Listen) ->
    %% io:format("new listen process:<<~p>>~n", self()),
    {ok, Socket} = gen_tcp:accept(Listen),
    spawn(fun() -> par_connect(Listen) end),
    handle_client_req(Socket).

handle_client_req(Socket) ->
    process_flag(trap_exit, true),

    receive
	{tcp, Socket, Bin} ->
	    Req = binary_to_term(Bin),
	    io:format("Server received req:~p~n", [Req]),

	    case Req of
		{read, ChunkID, Begin, Size} ->
		    read_response(Socket, ChunkID, Begin, Size);
		{write, ChunkID} ->
		    write_response(Socket, ChunkID);
		%% {stop, Why} ->
		%%    Child ! {stop, Why},
		%%    io:format("receive stop request from client, and transfer will be terminated~n");
		_Any ->
		    io:format("Server Unkown req:~p~n", [Req])
	    end;
	{tcp_close, Socket} ->
	    io:format("Control Socket was closed by Client~n");
	_Unknown ->
	    io:format("something unknown to server~n")
    end.

read_response(Socket, _ChunkID, Begin, Size) ->
    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    {ok, Port} = inet:port(ListenData),
    gen_tcp:send(Socket, term_to_binary({ok, Port})),

    {ok, SocketData} = gen_tcp:accept(ListenData),
    {ok, Hdl} = file:open("send.dat", [raw, read, binary]),
    send_it(SocketData, Hdl, Begin, Size),
    gen_tcp:close(SocketData).

send_it(SocketData, Hdl, Begin, Size) ->
    {ok, Binary} = file:pread(Hdl, Begin, Size),
    gen_tcp:send(SocketData, Binary).


write_response(_Socket, _ChunkID) ->
    io:format("Not implemented yet server:write_response/2~n").
