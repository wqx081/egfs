-module(data_reader).
-include("../include/egfs.hrl").
-include("data_server.hrl").
-import(chunk_db).
-import(toolkit, [get_file_size/1,
		  get_file_handle/2,
		  get_file_name/1,
		  get_local_addr/0]).
-export([handle_read/3]).

handle_read(ChunkID, Begin, Size) ->
    {ok, Name} = get_file_name(ChunkID),
    {ok, FileSize} = get_file_size(Name),

    if 
	(Begin >= FileSize) orelse (Size =< 0) ->
	    ?DEBUG("[data_server, ~p]: read boundary invalid ~p~n", [?LINE, Begin]),
	    Reply = {error, "invalid read args", []};
	true ->
	    if 
		Begin + Size > FileSize ->
		    End = FileSize;
		true ->
		    End = Begin + Size
	    end,

	    {ok, Listen} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
	    {ok, IP_Addr} = get_local_addr(),
	    {ok, Port} = inet:port(Listen),
	    Reply = {ok, IP_Addr, Port},
	    spawn(fun() -> read_process(Listen, ChunkID, Begin, End) end)
    end,

    Reply.

%% a read process
read_process(Listen, ChunkID, Begin, End) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    gen_tcp:close(Listen),
    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    Reply = inet:port(ListenData),
    gen_tcp:send(Socket, term_to_binary(Reply)),

    process_flag(trap_exit, true),
    Parent = self(),
    Child = spawn_link(fun() -> send_it(Parent, ListenData, ChunkID, Begin, End) end),

    loop_read_control(Socket, Child, 0).
    
loop_read_control(Socket, Child, State) ->
    receive 
	{finish, Child, Len} ->
	    ?DEBUG("[data_server, ~p]: read finish ~p Bytes~n", [?LINE, Len]),
	    gen_tcp:close(Socket);
	{tcp, Socket, Binary} ->
	    Term = binary_to_term(Binary),
	    case Term of 
		{stop, _Why} ->
		    ?DEBUG("[data_server, ~p]: stop msg from client ", [?LINE]),
		    %% Child ! {stop, Why};
		    exit(Child, kill);
		_Any ->
		    loop_read_control(Socket, Child, State)
	    end;
	{tcp_closed, Socket} ->
	    %% if the child is still alive, then it shuld be killed
	    void
    end.

send_it(Parent, ListenData, ChunkID, Begin, End) ->
    {ok, SocketData} = gen_tcp:accept(ListenData),
    gen_tcp:close(ListenData),
    {ok, Hdl} = get_file_handle(read, ChunkID),
    loop_send(Parent, SocketData, Hdl, Begin, End, 0),
    file:close(Hdl).

loop_send(Parent, SocketData, Hdl, Begin, End, Len) when Begin < End ->
    Size1  = End - Begin,
    if
	Size1 > ?STRIP_SIZE ->
	    Size = ?STRIP_SIZE;
	true->
	    Size = Size1
    end,

    {ok, Binary} = file:pread(Hdl, Begin, Size),
    gen_tcp:send(SocketData, Binary),
    Len2 = Len + size(Binary),
    Begin2 = Begin + size(Binary),
    loop_send(Parent, SocketData, Hdl, Begin2, End, Len2);
loop_send(Parent, SocketData, _Hdl, _Begin, _End, Len) ->
    gen_tcp:close(SocketData),
    Parent ! {finish, self(), Len},
    void.
