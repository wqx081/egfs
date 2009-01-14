-module(data_reader).
-include("../include/egfs.hrl").
-include("data_server.hrl").
-import(toolkit, [get_file_size/1,
		  get_file_handle/2,
		  get_file_name/1,
		  get_proper_size/3]).
-export([handle_read/3]).

handle_read(ChunkID, Begin, Size, {Listen, IP, Port}) ->
    {ok, End} = get_proper_end(ChunkID, Begin, Size),
    _Child = spawn(fun() -> read_process(Listen, ChunkID, Begin, End) end),
    {ok, IP, Port}.

get_proper_end(ChunkID, Begin, Size) ->
    {ok, Name} = toolkit:get_file_name(ChunkID),
    {ok, FileSize} = toolkit:get_file_size(Name),
    if 
	(Begin >= FileSize) orelse (Size =< 0) ->
	    ?DEBUG("[~p, ~p]: read boundary invalid ~p~n", [?MODULE, ?LINE, Begin]),
	    _Result = {error, bad_bundary, "invalid read begin or size"};
	true ->
	    Size2 = toolkit:get_proper_size(Begin, FileSize, Size),
	    End = Begin + Size2,
	    _Result = {ok, End}
    end.
    
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
    wait_for_another(Socket, Hdl).

wait_for_another(Socket, Hdl) ->
    receive
	{tcp, Socket, {readchunk, ChunkID, Begin, Size}} ->
	    gen_tcp:send(Socket, term_to_binary({ok, readchunk})),
	    loop_send(Socket, Hdl, Begin, Begin + Size);
	{tcp_closed, Socket} ->
	    void
    after
	20000 ->
	    gen_tcp:close(Socket);
    end.

