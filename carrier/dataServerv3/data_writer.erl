-module(data_writer).
-include("../include/egfs.hrl").
-include("data_server.hrl").
-import(chunk_db).
-import(toolkit).
-export([handle_write/6]).

handle_write(FileID, ChunkID, Begin, Size, {Listen, IP, Port}, _Nodelist) ->
    case check_boundary(ChunkID, Begin, Size) of
	true ->
	    spawn(fun() -> write_process(Listen, FileID, ChunkID, Begin, Size) end),
	    _Reply = {ok, IP, Port};
	_Other ->
	    _Reply = {error, bad_write, "bad write boundary"}
    end;
handle_write(_FileID, _ChunkID, _Begin, _Size, _Inet, [_H|_T]) ->
    {error, not_implemented}.

check_boundary(_ChunkId, _Begin, _Size) ->
    io:format("[~p, ~p] write check boundary not implemented~n", [?MODULE, ?LINE]),
    true.

write_process(Listen, FileID, ChunkID, Begin, Size) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    {ok, Hdl} = toolkit:get_file_handle(write, ChunkID),
    loop_receive(Socket, Hdl, Begin, Size, 0),
    file:close(Hdl),
    gen_tcp:close(Socket),
    insert_chunk(FileID, ChunkID).

loop_receive(Socket, Hdl, Begin, Size, Len) when Len < Size ->
    receive
	{tcp, Socket, Binary} ->
	    file:write(Hdl, Binary),
	    Shift = size(Binary),
	    Begin2 = Begin + Shift,
	    Len2 = Len + Shift,
	    loop_receive(Socket, Hdl, Begin2, Size, Len2);
	{tcp_closed, Socket} ->
	    %% toolkit:rm_pending_chunk(ChunkID),
	    {error, socket_broken, "socket broken when writing"};
	Any ->
	    ?DEBUG("[~p, ~p]:loop Any:~p~n", [?MODULE, ?LINE, Any]),
	    loop_receive(Socket, Hdl, Begin, Size, Len)
    end; 
loop_receive(Socket, Hdl, _Begin, _Size, _Len)  ->
    wait_for_another(Socket, Hdl).

wait_for_another(Socket, Hdl) ->
    receive
	{tcp, Socket, {writechunk, _FileID, ChunkID, Begin, Size, _Nodelist}} ->
	    check_boundary(ChunkID, Begin, Size),
	    gen_tcp:send(Socket, term_to_binary({ok, writechunk})),
	    loop_receive(Socket, Hdl, Begin, Size, 0);
	{tcp_closed, Socket} ->
	    void
    after
	20000 ->
	    gen_tcp:close(Socket)
    end.

insert_chunk(FileID, ChunkID) ->
    {ok, Name} = toolkit:get_file_name(ChunkID),
    {ok, FileSize} = toolkit:get_file_size(Name),
    io:format("[~p, ~p] ~p~n", [?MODULE, ?LINE, FileSize]),
    chunk_db:insert_chunk_info(ChunkID, FileID, Name, FileSize).

