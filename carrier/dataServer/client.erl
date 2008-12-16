-module(client).
-export([test/0, test_write/0]).

test() ->
    {ok, Socket} = gen_tcp:connect(localhost, 9999, [binary, {packet, 2}, {active, true}]),
    Read_req = {read, 2000, 0, 256},
    ok = gen_tcp:send(Socket, term_to_binary(Read_req)),

    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),

	    case Response of
		{ok, Port} ->
		    Child = spawn_link(receive_data(localhost, Port));
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
	{client_close} ->
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

