-module(client).
-include("../include/egfs.hrl").
-import(toolkit).
-export([write/0, read/0]).

-define(STRIP_SIZE, 8192).
-define(CHKID, <<0,0,172,10,0,9,103,237>>).
-define(Node, {data_server, lt@lt}).
-define(NextNode, {data_server, ltlt@lt}).
-define(SIZE, 1073741824). %% 2684354560

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%   write  & read tester
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
write() ->  
    FileID = 2000,
    ChunkIndex = 0,
    Nodelist = [?Node, ?NextNode],
    [H|T] = Nodelist,
    Reply = gen_server:call(H, {writechunk, FileID, ChunkIndex, ?CHKID, T}),
    io:format("[~p, ~p] Reply ~p~n", [?MODULE, ?LINE, Reply]),
    {ok, Host, Port} = Reply,
    Result = send_control(Host, Port),
    io:format("[~p, ~p] Result ~p~n", [?MODULE, ?LINE, Result]).

read() ->
    Reply = gen_server:call(?Node, {readchunk, ?CHKID, 0, ?SIZE}),
    io:format("Reply ~p~n", [Reply]),
    {ok, Host, Port} = Reply,
    Result = receive_control(Host, Port),
    io:format("Result ~p~n", [Result]).
    
    
%% send data to data server
send_control(Host, Port) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Binary} ->
	    {ok, Data_Port} = binary_to_term(Binary),
	    Parent = self(),
	    Child = spawn_link(fun() -> send_data(Host, Data_Port, Parent) end),
	    Result = transfer_control(Socket, Child);
	{tcp_closed, Socket} ->
	    Result = {error, control_connect, "control connect broken when waitin for data port"}
    end,
    gen_tcp:close(Socket),
    Result.

transfer_control(Socket, Child) ->
    receive 
	{finish, Child, Len} ->
	    Child ! {die, self()},
	    wait_for_check(Socket, Len);
	{tcp, Socket, Binary} ->
	    Term = binary_to_term(Binary),
	    case Term of
		{error, Why} ->
		    {error, control_connect, Why};
		_Any ->
		    transfer_control(Socket, Child)
	    end;
	{tcp_closed, Socket} ->
	    {error, control_connect, "control connect broken"}
    end.

wait_for_check(Socket, Len) ->
    receive
	{tcp, Socket, Binary} ->
	    Term = binary_to_term(Binary),
	    case Term of
		{check, Len} ->
		    Result = {ok, check},
		    gen_tcp:send(Socket, term_to_binary(Result)),
		    {ok, check, Len};
		_Other ->
		    Result = {error, check, "length error"},
		    gen_tcp:send(Socket, term_to_binary(Result)),
		    Result
	    end;
	{tcp_closed, Socket} ->
	    {error, check, "broken socket"};
	_Any ->
	    wait_for_check(Socket, Len)
    end.

send_data(Host, Port, Parent) ->
    {ok, Hdl} = file:open("hello_1.mp3", [binary, raw, read]),
    {ok, FileSize} = toolkit:get_file_size("hello_1.mp3"),

    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    io:format("Transfer begin: ~p~n", [erlang:time()]),
    loop_send(Parent, DataSocket, Hdl, 0, FileSize, 0),
    gen_tcp:close(DataSocket),
    io:format("Transfer end: ~p~n", [erlang:time()]).

loop_send(Parent, DataSocket, Hdl, Begin, End, Len) when Begin < End ->
    {ok, Binary} = file:pread(Hdl, Begin, ?STRIP_SIZE),
    gen_tcp:send(DataSocket, Binary),
    Size = size(Binary),
    Len2 = Len + Size,
    Begin2 = Begin + Size,
    loop_send(Parent, DataSocket, Hdl, Begin2, End, Len2);
loop_send(Parent, DataSocket, _, _, _, Len) ->
    Parent ! {finish, self(), Len},
    wait_to_die(Parent, DataSocket).
    
wait_to_die(Parent, DataSocket) ->
    receive 
	{die, Parent} ->
	    gen_tcp:close(DataSocket),
	    {ok, die};
	_Any ->
	    wait_to_die(Parent, DataSocket)
    end.

%% receive data from data server
receive_control(Host, Port) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    process_flag(trap_exit, true),
    receive
	{tcp, Socket, Binary} ->
	    {ok, Data_Port} = binary_to_term(Binary),
	    Parent = self(),
	    Child = spawn_link(fun() -> receive_data(Host, Data_Port, Parent) end),
	    Result = transfer_control_read(Socket, Child);
	{tcp_closed, Socket} ->
	    Result = {error, control_connect, "control connect broken when waitin for data port"}
    end,
    Result.

transfer_control_read(Socket, Child) ->
    receive
	{finish, Child, Len} ->	
	    ?DEBUG("[~p, ~p]:read a chunk, size is ~p.~n",[?MODULE, ?LINE, Len]),
	    gen_tcp:send(Socket, term_to_binary({check, data_length})),
	    wait_for_check_result(Socket, Len);
	{error, Child, Why} ->
	    ?DEBUG("[~p, ~p]:data receive socket error!~p~n",[?MODULE, ?LINE, Why]),
	    {error, read_data, Why};
        {tcp, Socket, Binary} -> 
            Term = binary_to_term(Binary),
	    case Term of
		{stop, Why} ->	    	    
		    ?DEBUG("[~p, ~p]:stop ctrl message from dataserver~n",[?MODULE, ?LINE]),
		    Child ! {stop, self(), Why},
		    {error, stop, "receive stop from data server"};
		_Other ->
		    ?DEBUG("[~p, ~p]:unknow msg from control socket:~p~n",[?MODULE, ?LINE, Term]),
		    transfer_control_read(Socket, Child)
	    end;
	{tcp_closed, Socket} ->	    
	    ?DEBUG("[~p, ~p]: read control socket broken~n",[?MODULE, ?LINE]),
	    {error, control_connect, "read control socket broken"};
	Any ->
	    ?DEBUG("[~p, ~p]:unknow messege!:~p~n",[?MODULE, ?LINE, Any]),
	    transfer_control_read(Socket, Child)
    end.

wait_for_check_result(Socket, Len) ->
    receive
	{tcp, Socket, Binary} ->
	    Term = binary_to_term(Binary),
	    case Term of
		{check, Len} ->
		    {ok, Len};
		{check, _Other} ->
		    {error, check_len_error, "data received length check error"};
		_Any ->
		    wait_for_check_result(Socket, Len)
	    end;
	{tcp_closed, Socket} ->
	    ?DEBUG("[~p, ~p] control socket broken when waiting for check result~n", [?MODULE, ?LINE]),
	    {error, control_socket, "Control socket broken waiting for check result"};
	_AnyOther ->
	    wait_for_check_result(Socket, Len)
    end.

receive_data(Host, Port, Parent) ->
    io:format("data port:~p~n", [Port]),
    {ok, Hdl} = file:open("recv.dat", [raw, append, binary]),

    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    io:format("Transfer begin: ~p~n", [erlang:time()]),
    loop_receive(Parent, DataSocket, Hdl, 0),
    gen_tcp:close(DataSocket),
    file:close(Hdl),
    io:format("Transfer end: ~p~n", [erlang:time()]).

loop_receive(Parent, DataSocket, Hdl, Len) ->
    receive
	{tcp, DataSocket, Data} ->
	    write(Data, Hdl),
	    Len2 = Len + size(Data),
	    loop_receive(Parent, DataSocket, Hdl, Len2);
	{tcp_closed, DataSocket} ->
	    Parent ! {finish, self(), Len},
	    io:format("read chunk over!~n");
	{stop, Parent, _Why} ->
	    gen_tcp:close(DataSocket)
    end.

write(Data, Hdl) ->
    file:write(Hdl, Data).
