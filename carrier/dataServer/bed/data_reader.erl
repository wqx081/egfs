-module(data_reader).
-include("../include/egfs.hrl").
-include("data_server.hrl").
-import(toolkit, [get_file_size/1,
		  get_file_handle/2,
		  get_file_name/1,
		  get_local_addr/0]).
-export([handle_read/3]).

handle_read(ChunkID, Begin, Size, {Listen, IP, Port}, CPid) ->
    {ok, Name} = toolkit:get_file_name(ChunkID),
    {ok, FileSize} = toolkit:get_file_size(Name),

    if 
	(Begin >= FileSize) orelse (Size =< 0) ->
	    ?DEBUG("[~p, ~p]: read boundary invalid ~p~n", [?MODULE, ?LINE, Begin]),
	    Reply = {error, "invalid read args", []};
	true ->
	    if 
		Begin + Size > FileSize ->
		    End = FileSize;
		true ->
		    End = Begin + Size
	    end,

	    PName = toolkit:get_proc_name(read, CPid, ChunkID),
	    case whereis(PName) of
		Pid ->
		    Pid ! {restart, ChunkID, Begin, End},
		    Reply = {ok, reuse};
		undefined ->    
		    Reply = {ok, IP, Port},
		    Child = spawn(fun() -> read_process(Listen, ChunkID, Begin, End) end),
		    register(PName, Child)
	    end
    end,

    Reply.

%% a read process
read_process(Listen, ChunkID, Begin, End) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    {ok, Hdl} = get_file_handle(read, ChunkID),
    loop_send(Socket, Hdl, Begin, End),
    gen_tcp:close(Socket),
    file:close(Hdl).
    
loop_send(Socket, Hdl, Begin, End) when Begin < End ->
    Size1  = End - Begin,
    if
	Size1 > ?STRIP_SIZE ->
	    Size = ?STRIP_SIZE;
	true->
	    Size = Size1
    end,

    {ok, Binary} = file:pread(Hdl, Begin, Size),
    gen_tcp:send(Socket, Binary),
    Begin2 = Begin + size(Binary),
    loop_send(Socket, Hdl, Begin2, End);
loop_send(SocketData, _Hdl, _Begin, _End) ->
    wait_for_another(Socket, Hdl),
    void.

wait_for_another(Socket, Hdl) ->
    receive
	{restart, _ChunkID, Begin, End} ->
	    loop_send(Socket, Hdl, Begin, End);
	{tcp_closed, Socket} ->
	    void
    after
	20000 ->
	    gen_tcp:close(Socket);
    end.

