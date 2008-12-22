-module(shout_server).
-include_lib("kernel/include/file.hrl").
-include("../include/egfs.hrl").
-export([start/0, stop/0]).

-define(STRIP_SIZE, 8192).
-define(DEF_PORT, 9999).
-define(TO_SEND, "hello.rmvb").
-define(TO_WRITE, "to_write.dat").

start() ->
    spawn(fun() -> 
		start_parallel_server(?DEF_PORT),
		lib_misc:sleep(infinity)
		end).

stop() ->
    case whereis(shout_server) of
	undefined ->
	    not_started;
	Pid ->
	    exit(Pid, kill),
	    (catch unregister(shout_server)),
	    stopped
    end.

start_parallel_server(Port) ->
    {ok, Listen} = gen_tcp:listen(Port, [binary, {packet, 2}, 
						 {reuseaddr, true},
						 {active, true}]),
    register(shout_server, self()),
    spawn(fun() -> par_connect(Listen) end).

par_connect(Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    spawn(fun() -> par_connect(Listen) end),
    inet:setopts(Socket, [binary, {packet, 2}, {nodelay, true}, {active, true}]),

    get_request(Socket).

get_request(Socket) ->
    receive
	{tcp, Socket, Bin} ->
	    %%TODO: verify that we have receive the full Req first.
	    Req = binary_to_term(Bin),
	    ?DEBUG("[shoutServer]: receive req ~p~n", [Req]),

	    case Req of
		{read, ChunkID, Begin, Size} ->
		    read_response(Socket, ChunkID, Begin, Size);
		{write, ChunkID} ->
		    write_response(Socket, ChunkID);
		_Any ->
		    ?DEBUG("[shoutServer]: unkown req ~p~n", [Req])
	    end;

	{tcp_close, Socket} ->
	    ?DEBUG("[shoutServer]: control connect closed by client~n", []);
	_Unkown ->
	    ?DEBUG("[shoutServer]: something unkown to me~n", [])
    end.

read_response(Socket, _ChunkID, Begin, Size) ->
    {ok, FileSize} = get_file_size(?TO_SEND),

    if 
	(Begin >= FileSize) orelse (Size =< 0) ->
	    gen_tcp:send(Socket, term_to_binary({error, "invalid read boundary"})),
	    ?DEBUG("[shoutServer]: read bundary invalid ~p~n", [Begin]);
	true ->
	    if 
		Begin + Size > FileSize ->
		    End = FileSize;
		true ->
		    End = Begin + Size
	    end,

	    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
	    Reply = inet:port(ListenData), %% Reply = {ok, Port}
	    gen_tcp:send(Socket, term_to_binary(Reply)),

	    {ok, SocketData} = gen_tcp:accept(ListenData),
	    {ok, Hdl} = file:open(?TO_SEND, [binary, raw, read]),

	    send_it(SocketData, Hdl, Begin, End)
    end.

send_it(SocketData, Hdl, Begin, End) when Begin < End ->
    {ok, Binary} = file:pread(Hdl, Begin, ?STRIP_SIZE),
    gen_tcp:send(SocketData, Binary),
    Begin2 = Begin + ?STRIP_SIZE,
    send_it(SocketData, Hdl, Begin2, End);
send_it(_, _, _, _) ->
    void.

get_file_size(File) ->
    case file:read_file_info(File) of
	{ok, Facts} ->
	    {ok, Facts#file_info.size};
	_ ->
	    error
    end.


write_response(Socket, _ChunkID) ->
    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    Reply = inet:port(ListenData),
    gen_tcp:send(Socket, term_to_binary(Reply)),

    {ok, SocketData} = gen_tcp:accept(ListenData),
    {ok, Hdl} = file:open(?TO_WRITE, [binary, raw, write]),

    process_flag(trap_exit, true),
    Child = spawn_link(fun() -> receive_data(self(), SocketData, Hdl, 0) end),

    loop_receive_control(Child, Socket).

loop_receive_control(Child, Socket) ->
    receive
	{tcp, Socket, Binary} ->
	    Term = binary_to_term(Binary),
	    case Term of
		{finish, Size} ->
		    ?DEBUG("[shoutServer]: notify from client that the receive porcess finished, size = ~p~n", Size);
		{stop, _Why} ->
		    ?DEBUG("[shoutServer]: notify from client that the receive process should be stopped~n", []),
		    Child ! {stop, _Why};
		_ ->
		    ?DEBUG("[shoutServer]: unkown notify from client :~p~n", Term)
	    end;
	{tcp_close, Socket} ->
	    ?DEBUG("[shoutServer]: client has closed the controll socket", []);
	{finish, Child, ChunkID, Hdl, Len} ->
	    case check_it(Socket, ChunkID, Len) of
		{ok, _} ->
		    report_to_metaServer(ChunkID, Len);
		{error, _Why} ->
		    file:delete(Hdl)
	    end
    end.

receive_data(Parent, SocketData, Hdl, Len) ->
    receive
	{tcp, SocketData, Binary} ->
	    Len2 = Len + size(Binary),
	    write_file(Hdl, Binary),
	    receive_data(SocketData, Hdl, Len2);
	{tcp_close, _Why} ->
	    file:delete(?TO_WRITE),
	    ?DEBUG("[shoutServer]: client close socket.~n", []);
	{stop, Parent} ->
	    ?DEBUG("[shoutServer]: stop signal from parent~n", [])
    end.

check_it(Socket, ChunkID, Len) ->
    ?DEBUG("[shoutServer]: check the chunk, not implemented yet~n", []),
    {ok, "Good"}.

report_to_metaServer(ChunkID, Len) ->
    ?DEBUG("[shoutServer]: report to the metaServer~n", []).
