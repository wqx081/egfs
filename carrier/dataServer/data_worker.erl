-module(data_worker).
-include("../include/egfs.hrl").
-import(chunk_db).
-import(toolkit, [get_file_size/1,
		  get_file_handle/2,
		  get_file_name/1,
		  get_local_addr/0,
		  rm_pending_chunk/1,
		  report_metaServer/4]).

-export([handle_read/3, handle_write/4]).

-define(STRIP_SIZE, 8192).

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

handle_write(FileID, ChunkIndex, ChunkID, _Nodelist) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    {ok, IP_Addr} = get_local_addr(),
    {ok, Port} = inet:port(Listen),
    Reply = {ok, IP_Addr, Port},
    spawn(fun() -> write_process(FileID, ChunkIndex, Listen, ChunkID) end),

    Reply.

%% a write process    
write_process(FileID, ChunkIndex, Listen, ChunkID) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    Reply = inet:port(ListenData),
    gen_tcp:send(Socket, term_to_binary(Reply)),

    process_flag(trap_exit, true),
    Parent = self(),
    Child = spawn_link(fun() -> receive_it(Parent, ListenData, ChunkID) end),

    loop_write_control(Socket, Child, FileID, ChunkIndex, ChunkID, 0),

    ?DEBUG("[data_server, ~p]: write control process finished !~n", [?LINE]).

loop_write_control(Socket, Child, FileID, ChunkIndex, ChunkID, State) ->
    receive
	{finish, Child, Len} ->
	    %% {ok, _Info} = check_it(Socket, ChunkID, Len),
	    ?DEBUG("[data_server, ~p]: write transfer finish, ~pBytes~n", [?LINE, Len]),
	    {ok, Name} = get_file_name(ChunkID),
	    chunk_db:insert_chunk_info(ChunkID, FileID, Name, Len),
	    {ok, _Info} = report_metaServer(FileID, ChunkIndex, ChunkID, Len);
	{tcp, Socket, Binary} ->
	    ?DEBUG("[data_server, ~p]: tcp, socket, binary~n", [?LINE]),
	    Term = binary_to_term(Binary),
	    case Term of 
		{stop, _Why} ->
		    ?DEBUG("[data_server, ~p]: write stop msg from client.~n", [?LINE]),
		    exit(Child, kill),
		    rm_pending_chunk(ChunkID);
		{finish, _ChunkID} ->
		    ?DEBUG("[data_server, ~p]: write control receive finish signal~n", [?LINE]),
		    State2 = State + 1,
		    loop_write_control(Socket, Child, FileID, ChunkIndex, ChunkID, State2);
		_Any ->
		    loop_write_control(Socket, Child, FileID, ChunkIndex, ChunkID, State)
	    end;
	{tcp_closed, Socket} ->
	    ?DEBUG("[data_server, ~p]: control tcp_closed~n", [?LINE]),
	    if
		State > 0 ->
		    loop_write_control(Socket, Child, FileID, ChunkIndex, ChunkID, State);
		true ->
		    ?DEBUG("[data_server, ~p]: write control broken~n", [?LINE]),
		    %% exit(Child, kill),
		    rm_pending_chunk(ChunkID)
	    end;
	{error, Child, Why} ->
	    ?DEBUG("[data_server, ~p]: data transfer socket error~p~n", [?LINE, Why]),
	    rm_pending_chunk(ChunkID);
	Any ->
	    ?DEBUG("[data_server, ~p]: unkown msg ~p~n", [?LINE, Any]),
	    loop_write_control(Socket, Child, FileID, ChunkIndex, ChunkID, State)
    end.

receive_it(Parent, ListenData, ChkID) ->
    {ok, SocketData} = gen_tcp:accept(ListenData),
    {ok, Hdl} = get_file_handle(write, ChkID),

    loop_receive(Parent, SocketData, Hdl, 0).

loop_receive(Parent, SocketData, Hdl, Len) ->
    receive
	{tcp, SocketData, Binary} ->
	    Len2 = Len + size(Binary),
	    file:write(Hdl, Binary),
	    loop_receive(Parent, SocketData, Hdl, Len2);
	{tcp_closed, SocketData} ->
	    Parent ! {finish, self(), Len},
	    file:close(Hdl);
	Any ->
	    ?DEBUG("[data_server, ~p]:loop Any:~p~n", [?LINE, Any])
    end. 

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%                     kits
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% get_local_addr() ->
%%    {ok, Host} = inet:gethostname(),
%%    inet:getaddr(Host, inet).
