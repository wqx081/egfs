-module(data_writer).
-include("../include/egfs.hrl").
-include("data_server.hrl").
-import(chunk_db).
-import(toolkit).
-export([handle_write/4]).

handle_write(FileID, ChunkIndex, ChunkID, {Listen, IP, Port}, CPid, []) ->
    PName = toolkit:get_proc_name(write, CPid, ChunkID),
    case whereis(PName) of
	Pid ->
	    Reply = {ok, reuse};
	undefined ->    
	    Reply = {ok, IP, Port},
	    Child = spawn(fun() -> write_process(FileID, ChunkIndex, ChunkID, Listen) end),
	    register(PName, Child)
    end
    Reply;
handle_write(FileID, ChunkIndex, ChunkID, {Listen, IP, Port}, CPid, [H|T]) ->
    {error, not_implemented}.

write_process(FileID, ChunkIndex, ChunkID, Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    {ok, Hdl} = toolkit:get_file_handle(write, ChkID),
    {ok, Len} = loop_receive(SocketData, Hdl, 0),
    io:format("[~p, ~p] ~p~n", [?MODULE, ?LINE, Len]),
    file:close(Hdl),
    gen_tcp:close(SocketData),
    insert_chunk(FileID, ChunkID).

loop_receive(SocketData, Hdl, Len) ->
    receive
	{tcp, SocketData, Binary} ->
	    Len2 = Len + size(Binary),
	    file:write(Hdl, Binary),
	    loop_receive(SocketData, Hdl, Len2);
	{tcp_closed, SocketData} ->
	    {ok, Len};
	{stop, data_gen_server, _Why} ->
	    {ok, Len};
	Any ->
	    ?DEBUG("[~p, ~p]:loop Any:~p~n", [?MODULE, ?LINE, Any]),
	    loop_receive(SocketData, Hdl, Len)
    after
	20000 ->
	    {ok, Len}
    end. 

insert_chunk(FileID, ChunkID) ->
    {ok, Name} = toolkit:get_file_name(ChunkID),
    {ok, FileSize} = toolkit:get_file_size(Name),
    io:format("[~p, ~p] ~p~n", [?MODULE, ?LINE, FileSize]),
    chunk_db:insert_chunk_info(ChunkID, FileID, Name, Len).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_write_process(Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    gen_tcp:close(Listen),
    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    Reply = inet:port(ListenData),
    gen_tcp:send(Socket, term_to_binary(Reply)),
    process_flag(trap_exit, true),
    Parent = self(),
    {ok, Socket, ListenData, Parent}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%            Write to Itself
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
write_process(FileID, ChunkIndex, ChunkID, Listen) ->
    {ok, Socket, ListenData, Parent} = init_write_process(Listen),
    Child = spawn_link(fun() -> receive_it_tail(Parent, ListenData, ChunkID) end),
    Result = loop_write_control_tail(Socket, Child, FileID, ChunkIndex, ChunkID, 0),
    io:format("[~p, ~p] write control finish: ~p~n", [?MODULE, ?LINE, Result]),
    write_aftercare(Result, ChunkID),
    io:format("[~p, ~p] write_process finish~n", [?MODULE, ?LINE]).

loop_write_control_tail(Socket, Child, FileID, ChunkIndex, ChunkID, State) ->
    receive
	{finish, Child, Len} ->
	    ?DEBUG("[~p, ~p]: write transfer finish, ~pBytes~n", [?MODULE, ?LINE, Len]),
	    {ok, Name} = toolkit:get_file_name(ChunkID),
	    chunk_db:insert_chunk_info(ChunkID, FileID, Name, Len),
	    gen_tcp:send(Socket, term_to_binary({check, Len})),
	    wait_for_check_result(Socket);
	{error, Child, Why} ->
	    ?DEBUG("[~p, ~p]: data transfer error~p~n", [?MODULE, ?LINE, Why]),
	    {error, data_receive, Why};
	{tcp, Socket, Binary} ->
	    Term = binary_to_term(Binary),
	    case Term of 
		{stop, Why} ->
		    ?DEBUG("[data_server, ~p]: write stop msg from client.~n", [?LINE]),
		    Child ! {stop, self(), Why},
		    gen_tcp:close(Socket),
		    {ok, stop, "write stop from writer"};
		_Any ->
		    loop_write_control_tail(Socket, Child, FileID, ChunkIndex, ChunkID, State)
	    end;
	{tcp_closed, Socket} ->
	    ?DEBUG("[~p, ~p]: write control broken~n", [?MODULE, ?LINE]),
	    {error, write_control, "write control broken"};
	Any ->
	    ?DEBUG("[~p, ~p]: unkown msg ~p~n", [?MODULE, ?LINE, Any]),
	    loop_write_control_tail(Socket, Child, FileID, ChunkIndex, ChunkID, State)
    end.

receive_it_tail(Parent, ListenData, ChkID) ->
    {ok, SocketData} = gen_tcp:accept(ListenData),
    gen_tcp:close(ListenData),
    {ok, Hdl} = toolkit:get_file_handle(write, ChkID),
    loop_receive(Parent, SocketData, Hdl, 0),
    gen_tcp:close(SocketData).

loop_receive(Parent, SocketData, Hdl, Len) ->
    receive
	{tcp, SocketData, Binary} ->
	    Len2 = Len + size(Binary),
	    file:write(Hdl, Binary),
	    loop_receive(Parent, SocketData, Hdl, Len2);
	{tcp_closed, SocketData} ->
	    file:close(Hdl),
	    Parent ! {finish, self(), Len};
	{stop, Parent, _Why} ->
	    file:close(Hdl),
	    get_tcp:close(SocketData);
	Any ->
	    ?DEBUG("[data_server, ~p]:loop Any:~p~n", [?LINE, Any]),
	    loop_receive(Parent, SocketData, Hdl, Len)
    end. 

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%            Relay Write
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect_to_next({ok, Next_IP, Next_Port}) ->
    {ok, NextSocket} = gen_tcp:connect(Next_IP, Next_Port, [binary, {packet, 2}, {active, true}]),
    receive 
	{tcp, NextSocket, Binary} ->
	    {ok, Next_DataPort} = binary_to_term(Binary),
	    {ok, NextSocket, Next_IP, Next_DataPort};
	_Any ->
	    {error, next_connect, "cann't get next data port"}
    end.
	    
relay_write_process(FileID, ChunkIndex, ChunkID, Listen, Next) ->
    {ok, Socket, ListenData, Parent} = init_write_process(Listen),
    {ok, NextSocket, Next_IP, Next_DataPort} = connect_to_next(Next),
    Child = spawn_link(fun() -> relay_receive_it(Parent, ListenData, Next_IP, Next_DataPort, ChunkID) end),
    Result = loop_relay_write_control(Socket, NextSocket, Child, FileID, ChunkIndex, ChunkID, 0),
    io:format("[~p, ~p] write control finish: ~p~n", [?MODULE, ?LINE, Result]),
    write_aftercare(Result, ChunkID),
    io:format("[~p, ~p] write_process finish~n", [?MODULE, ?LINE]),
    gen_tcp:close(NextSocket).

write_aftercare(Result, ChunkID) ->
    case Result of
	{ok, check} ->
	    ok;
	_Any ->
	    toolkit:rm_pending_chunk(ChunkID),
	    chunk_db:remove_chunk_info(ChunkID)
    end.

loop_relay_write_control(Socket, NextSocket, Child, FileID, ChunkIndex, ChunkID, State) ->
    receive
	{finish, Child, Len} ->
	    {ok, Name} = toolkit:get_file_name(ChunkID),
	    chunk_db:insert_chunk_info(ChunkID, FileID, Name, Len),
	    %% Result = report_metaServer(FileID, ChunkIndex, ChunkID, Len),
	    Child ! {die, self()},
	    wait_for_check(Socket, NextSocket, ChunkID, Len, 0);
	{error, Child, Why} ->
	    ?DEBUG("[~p, ~p] error from child(data transfer) ~n", [?MODULE, ?LINE]),
	    toolkit:rm_pending_chunk(ChunkID),
	    gen_tcp:send(Socket, term_to_binary({error, "data transfer error"})),
	    gen_tcp:send(NextSocket, term_to_binary({stop, "data transfer error"})),
	    gen_tcp:close(NextSocket),
	    Child ! {die, self()},
	    {error, data_receive, Why};
	{'EXIT', _, normal} ->
	    loop_relay_write_control(Socket, NextSocket, Child, FileID, ChunkIndex, ChunkID, State);
	{tcp, Socket, Binary} ->
	    gen_tcp:send(NextSocket, Binary),
	    loop_relay_write_control(Socket, NextSocket, Child, FileID, ChunkIndex, ChunkID, State);
	{tcp, NextSocket, Binary} ->
	    gen_tcp:send(Socket, Binary),
	    loop_relay_write_control(Socket, NextSocket, Child, FileID, ChunkIndex, ChunkID, State);
	{tcp_closed, Socket} ->
	    toolkit:rm_pending_chunk(ChunkID),
	    gen_tcp:send(NextSocket, term_to_binary({stop, "data control error"})),
	    gen_tcp:close(NextSocket),
	    {error, control_socket, "control socket broken"};
	{tcp_closed, NextSocket} ->
	    %%rm_pending_chunk(ChunkID),
	    gen_tcp:send(Socket, term_to_binary({error, "next data control broken"})),
	    {error, next_control_socket, "next control socket broken"};
	Any ->
	    ?DEBUG("[~p, ~p] unkown control msg:~p ~n", [?MODULE, ?LINE, Any]),
	    loop_relay_write_control(Socket, NextSocket, Child, FileID, ChunkIndex, ChunkID, State)
    end.

wait_for_check(Socket, NextSocket, ChunkID, Len, State) ->
    receive
	{tcp, NextSocket, Binary} ->
	    Term = binary_to_term(Binary),
	    case Term of
		{check, Len} ->
		    Result = {ok, check},
		    gen_tcp:send(NextSocket, term_to_binary(Result)),
		    gen_tcp:send(Socket, Binary),
		    wait_for_check_result(Socket);
		_Other ->
		    Result = {error, check, "length error"},
		    gen_tcp:send(Socket, term_to_binary(Result)),
		    gen_tcp:send(NextSocket, term_to_binary(Result)),
		    Result
	    end;
	_Any ->
	    wait_for_check(Socket, NextSocket, ChunkID, Len, State)
    end.

wait_for_check_result(Socket) ->
    receive
	{tcp, Socket, Binary} ->
	    Term = binary_to_term(Binary),
	    case Term of
		{ok, check} ->
		    Term;
	        {error, check, _Why} ->
		    Term;
		Other ->
		    {error, check, Other}
	    end;
	{tcp_closed, Socket} ->
	    {error, check, "broken socket"};
	_Any ->
	    wait_for_check_result(Socket)
    end.

relay_receive_it(Parent, ListenData, Next_IP, Next_DataPort, ChunkID) ->
    {ok, SocketData} = gen_tcp:accept(ListenData),
    gen_tcp:close(ListenData),
    {ok, Hdl} = toolkit:get_file_handle(write, ChunkID),
    {ok, NextSocketData} = gen_tcp:connect(Next_IP, Next_DataPort, [binary, {packet, 2}, {active, true}]),
    loop_relay_receive(Parent, SocketData, NextSocketData, Hdl, 0),
    gen_tcp:close(NextSocketData).

loop_relay_receive(Parent, SocketData, NextSocketData, Hdl, Len) ->
    receive
	{tcp, SocketData, Binary} ->
	    Len2 = Len + size(Binary),
	    file:write(Hdl, Binary),
	    gen_tcp:send(NextSocketData, Binary),
	    loop_relay_receive(Parent, SocketData, NextSocketData, Hdl, Len2);
	{tcp_closed, SocketData} ->
	    file:close(Hdl),
	    Parent ! {finish, self(), Len},
	    wait_to_die(Parent, NextSocketData, 0);
	{tcp_closed, NextSocketData} ->
	    ?DEBUG("[~p, ~p] next data socket broken ~n", [?MODULE, ?LINE]),
	    file:close(Hdl),
	    Parent ! {error, self(), "next socket data closed"},
	    wait_to_die(Parent, NextSocketData, 0);
	Any ->
	    ?DEBUG("[~p, ~p] unknown: ~p~n", [?MODULE, ?LINE, Any]),
	    loop_relay_receive(Parent, SocketData, NextSocketData, Hdl, Len)
    end.

wait_to_die(Parent, NextSocketData, State) ->
    receive
	{die, Parent} ->
	    gen_tcp:close(NextSocketData),
	    ?DEBUG("[~p, ~p] close next socket data~n", [?MODULE, ?LINE]); 
	Any ->
	    ?DEBUG("[~p, ~p] unknown when wait for death: ~p~n", [?MODULE, ?LINE, Any]),
	    wait_to_die(Parent, NextSocketData, State)
    end.
	    
