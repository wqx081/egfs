-module(client).
-export([test/0, test_write/0]).
-define(HOST, "192.168.0.111").

test() ->
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

receive_data(Host, Port) ->
    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    loop(DataSocket).

loop(DataSocket) ->
    receive
	{tcp, DataSocket, Data} ->
	    write(Data),
	    loop(DataSocket);
	{tcp_closed, DataSocket} ->
	    io:format("read chunk over!~n");
	{client_close, _Why} ->
	    io:format("client close the datasocket~n"),
	    gen_tcp:close(DataSocket)
    end.

write(Data) ->
    {ok, Hdl} = file:open("recv.dat", [raw, append, binary]),
    file:write(Hdl, Data),
    file:close(Hdl).

test_write() ->
    {ok, Hdl} = file:open("send.dat", [raw, read, binary]),
    {ok, Binary} = file:pread(Hdl, 5, 4),
    %%{ok, Binary} = file:read_file("send.dat"),
    write(Binary).

