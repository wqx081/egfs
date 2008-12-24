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

%% -record(chunk_info, {chunkID, location, fileID, index}).
-define(TABLE, "chunk_table").

start_link() -> gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

init([]) -> 
    {ok, ?TABLE} = dets:open_file(?TABLE, [{file, ?TABLE}]),
    ok = check_dirs(),
    {ok, server_has_startup}.

handle_call({readchunk, ChkID, Begin, Size}, _From, N) ->
    io:format("[data_server]: read request from client, chunkID(~p)~n", [ChkID]),
    {ok, Name} = get_file_name(ChkID),
    {ok, FileSize} = get_file_size(Name),
    %% {ok, FileSize} = get_file_size(?TO_SEND),

    if 
	(Begin >= FileSize) orelse (Size =< 0) ->
	    ?DEBUG("[shoutServer]: read boundary invalid ~p~n", [Begin]),
	    Reply = {error, "invalid read boundary"};
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
handle_call({writechunk, FileID, ChunkIndex, ChunkID, _Nodelist}, _From, N) ->
    io:format("[data_server]: write request from client, chkID(~p)~n", [ChunkID]),

    {ok, Listen} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    {ok, Host} = inet:getaddr(lt, inet),
    {ok, Port} = inet:port(Listen),
    Reply = {ok, Host, Port},
    spawn(fun() -> write_process(FileID, ChunkIndex, Listen, ChunkID) end),
    {reply, Reply, N};
handle_call(Msg, _From, N) ->
    ?DEBUG("[data_server]: unknown request ~p~n", [Msg]),
    {noreply, N}.
    
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

    ?DEBUG("write control process finished !~n", []).

loop_write_control(Socket, Child, FileID, ChunkIndex, ChunkID, _State) ->
    receive
	{finish, Child, Len} ->
	    %% {ok, _Info} = check_it(Socket, ChunkID, Len),
	    ?DEBUG("[data_server]: write transfer finish, ~pBytes~n", [Len]),
	    {ok, _Info} = report_metaServer(FileID, ChunkIndex, ChunkID, Len);
	{tcp, Socket, Binary} ->
	    ?DEBUG("[data_server]: tcp, socket, binary~n", []),
	    Term = binary_to_term(Binary),
	    case Term of 
		{stop, _Why} ->
		    ?DEBUG("[data_server]: write stop msg from client.~n", []),
		    exit(Child, kill),
		    rm_pending_chunk(ChunkID);
		_Any ->
		    loop_write_control(Socket, Child, FileID, ChunkIndex, ChunkID, _State)
	    end;
	{tcp_closed, Socket} ->
	    ?DEBUG("[data_server]: control tcp_closed~n", []),
	    %% exit(Child, kill),
	    rm_pending_chunk(ChunkID);
	{error, Child, Why} ->
	    ?DEBUG("[data_server]: data transfer socket error~p~n", [Why]),
	    rm_pending_chunk(ChunkID);
	Any ->
	    ?DEBUG("[data_server]: unkown msg ~p~n", [Any]),
	    loop_write_control(Socket, Child, FileID, ChunkIndex, ChunkID, _State)
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
	    io:format("loop Any:~p~n", [Any])
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
	{tcp_closed, Socket} ->
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
loop_send(SocketData, _, _, _) ->
    gen_tcp:close(SocketData),
    void.

%% utilites
get_file_handle(read, ChunkID) ->
    {ok, Name} = get_file_name(ChunkID),
    {ok, _Hdl} = file:open(Name, [binary, raw, read, read_ahead]);
get_file_handle(write, ChunkID) ->
    {ok, Name} = get_file_name(ChunkID),
    {ok, Hdl} = file:open(Name, [binary, raw, append]),
    file:truncate(Hdl),
    {ok, Hdl}.

get_file_name(ChunkID) when is_binary(ChunkID) ->
    <<D:32, Fn:32>> = ChunkID,
    Dir = integer_to_list(D rem 10),
    Result = {ok, lists:append(["./data/", Dir, "/", integer_to_list(Fn)])},
    Result;
get_file_name(_) ->
    {error, "invalid ChunkID to convert to name"}.

temp(ChunkID) ->
    <<D1:4, D2:4, D3:4, D4:4, _Fn:48>> = ChunkID,
    generate_dirs("./data", [D1, D2, D3, D4]).


generate_dirs(Cur, [H|T]) ->
    Cur2 = lists:append([Cur, "/", [H]]),
    Bool = filelib:is_dir(Cur2),
    if
	not(Bool) ->
	    file:make_dir(Cur2);
	true ->
	    void
    end,
    generate_dirs(Cur2, [T]);
generate_dirs(Cur, []) ->
    Cur2 = lists:append([Cur, "/"]).

get_file_size(FileName) ->
    case file:read_file_info(FileName) of
	{ok, Facts} ->
	    {ok, Facts#file_info.size};
	Other ->
	    Other
    end.

rm_pending_chunk(ChunkID) ->
    io:format("[dataserver]: has removed ~p~n", ChunkID),
    {ok, "has removed it"}.

report_metaServer(FileID, _ChunkIndex, ChunkID, Len) ->
    io:format("[dataserver]: reporting to metaserver (~p, ~p)~n", [ChunkID, Len]),
    gen_server:call({global, metagenserver}, {registerchunk, FileID, ChunkID, Len, []}),
    {ok, "has reported it"}.

%%
check_dirs() ->
    ok.
%% 
handle_cast(_Msg, N) -> {noreply, N}.
handle_info(_Info, N) -> {noreply, N}.

terminate(_Reason, _N) ->
    ?DEBUG("~p is stopping~n", [?MODULE]),
    dets:close(?TABLE),
    ok.

code_change(_OldVsn, N, _Extra) -> {ok, N}.

