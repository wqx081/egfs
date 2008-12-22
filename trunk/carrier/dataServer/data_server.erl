-module(data_server).
-behaviour(gen_server).
-include_lib("kernel/include/file.hrl").
-include("../include/egfs.hrl").
-export([start_link/0, 
	 init/1, handle_call/3,
	 handle_cast/2, handle_info/2, 
	 terminate/2, code_change/3]).

-define(STRIP_SIZE, 8192).
-define(DEF_PORT, 9999).
-define(TO_SEND, "send.dat").
-define(TO_WRITE, "to_write.dat").

start_link() -> gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

init([]) -> 
    {ok, server_has_startup}.

handle_call({read, ChkID, Begin, Size}, _From, N) ->
    io:format("[data_server]: read request from client~n"),
    {ok, FileSize} = get_file_size(?TO_SEND),

    if 
	(Begin >= FileSize) orelse (Size =< 0) ->
	    ?DEBUG("[shoutServer]: read bundary invalid ~p~n", [Begin]),
	    Reply = {error, "invalid read bundary"};
	true ->
	    if 
		Begin + Size > FileSize ->
		    End = FileSize;
		true ->
		    End = Begin + Size
	    end,

	    {ok, Listen} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
	    {ok, Host} = inet:getaddr(lt, inet),
	    {ok, Port} = inet:port(Listen),
	    Reply = {ok, Host, Port},
	    spawn(fun() -> read_process(Listen, ChkID, Begin, End) end)
    end,
    {reply, Reply, N};
handle_call({write, {chunkID, ChkID}}, _From, N) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    Reply = inet:sockname(Listen),
    spawn(fun() -> write_process(Listen, ChkID) end),
    {reply, Reply, N};
handle_call(Msg, _From, N) ->
    ?DEBUG("[data_server]: unknown request ~p~n", [Msg]),
    {noreply, N}.
    
%% a write process    
write_process(Listen, ChunkID) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    Reply = inet:port(ListenData),
    gen_tcp:send(Socket, term_to_binary(Reply)),

    {ok, Hdl} = get_file_handle(write, ChunkID),
    process_flag(trap_exit, true),
    Child = spawn_link(fun() -> receive_it(self(), ListenData, Hdl) end),

    loop_write_control(Socket, Child, ChunkID, 0).

loop_write_control(Socket, Child, ChunkID, _State) ->
    receive
	{finish, Child, Len} ->
	    %% {ok, _Info} = check_it(Socket, ChunkID, Len),
	    {ok, _Info} = report_metaServer(ChunkID, Len);
	{tcp, Socket, Binary} ->
	    Term = binary_to_term(Binary),
	    case Term of 
		{stop, _Why} ->
		    ?DEBUG("[data_server]: write stop msg from client.~n", []),
		    exit(Child, kill),
		    rm_pending_chunk(ChunkID);
		_Any ->
		    loop_write_control(Socket, Child, ChunkID, _State)
	    end;
	{tcp_close, Socket} ->
	    exit(Child, kill),
	    rm_pending_chunk(ChunkID);
	{error, Child, Why} ->
	    ?DEBUG("[data_server]: data transfer socket error~p~n", [Why]),
	    rm_pending_chunk(ChunkID);
	Any ->
	    ?DEBUG("[data_server]: unkown msg ~p~n", [Any])
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
	{tcp_close, SocketData} ->
	    file:close(Hdl),
	    Parent ! {finish, self(), Len}
    end.
	    
%% a read process
read_process(Listen, ChunkID, Begin, End) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    Reply = inet:port(ListenData),
    gen_tcp:send(Socket, term_to_binary(Reply)),

    process_flag(trap_exit, true),
    Child = spawn_link(fun() -> send_it(ListenData, ChunkID, Begin, End) end),

    loop_read_control(Socket, Child, 0).
    
loop_read_control(Socket, Child, State) ->
    receive 
	{tcp, Socket, Binary} ->
	    Term = binary_to_term(Binary),
	    case Term of 
		{stop, _Why} ->
		    ?DEBUG("[data_server]: stop msg from client ", []),
		    %% Child ! {stop, Why};
		    exit(Child, kill);
		_Any ->
		    loop_read_control(Socket, Child, State)
	    end;
	{tcp_close, Socket} ->
	    %% if the child is still alive, then it shuld be killed
	    void
    end.

send_it(ListenData, ChunkID, Begin, End) ->
    {ok, SocketData} = gen_tcp:accept(ListenData),
    {ok, Hdl} = get_file_handle(read, ChunkID),
    loop_send(SocketData, Hdl, Begin, End),
    file:close(Hdl).

loop_send(SocketData, Hdl, Begin, End) when Begin < End ->
    {ok, Binary} = file:pread(Hdl, Begin, ?STRIP_SIZE),
    gen_tcp:send(SocketData, Binary),
    Begin2 = Begin + ?STRIP_SIZE,
    loop_send(SocketData, Hdl, Begin2, End);
loop_send(_, _, _, _) ->
    void.

%% utilites
get_file_handle(read, _ChunkID) ->
    {ok, _Hdl} = file:open(?TO_SEND, [binary, raw, read, read_ahead]);
get_file_handle(write, _ChunkID) ->
    {ok, Hdl} = file:open(?TO_WRITE, [binary, raw, append]),
    file:truncate(Hdl),
    {ok, Hdl}.

get_file_size(FileName) ->
    case file:read_file_info(FileName) of
	{ok, Facts} ->
	    {ok, Facts#file_info.size};
	_ ->
	    error
    end.

rm_pending_chunk(ChunkID) ->
    io:format("[dataserver]: has removed ~p~n", ChunkID),
    {ok, "has removed it"}.

report_metaServer(ChunkID, Len) ->
    io:format("[dataserver]: reporting to metaserver (~p, ~p)~n", ChunkID, Len),
    {ok, "has reported it"}.

%% 
handle_cast(_Msg, N) -> {noreply, N}.
handle_info(_Info, N) -> {noreply, N}.

terminate(_Reason, _N) ->
    ?DEBUG("~p is stopping~n", [?MODULE]),
    ok.

code_change(_OldVsn, N, _Extra) -> {ok, N}.

