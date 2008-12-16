-module(server).
-export([start/0]).

start() ->
    {ok, Listen} = gen_tcp:listen(9999, [binary, {packet, 2}, {active, true}]),

    spawn(fun() -> par_connect(Listen) end).

par_connect(Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    spawn(fun() -> par_connect(Listen) end),
    loop(Socket).

loop(Socket) ->
    receive
	{tcp, Socket, Bin} ->
	    Req = binary_to_term(Bin),
	    io:format("Server received req:~p~n", [Req]),

	    case Req of
		{read, ChunkID, Begin, Size} ->
		    do_read(Socket, ChunkID, Begin, Size);
		{write, ChunkID} ->
		    do_write(Socket, ChunkID);
		_Any ->
		    io:format("Server Unkown req:~p~n", [Req])
	    end
    end.
