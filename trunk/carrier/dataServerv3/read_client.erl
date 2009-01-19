-module(read_client).
-include("../include/egfs.hrl").
-include("data_server.hrl").
-import(toolkit, [get_proper_size/3,
		  timestamp/0]).
-export([pread/3, open/2, close/1, delete/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%Context = {FileID, FileSize, 
%%		 ChunkIndex, ChunkID, 
%%		 Nodelist,
%%		 Socket, Time, Pid}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
pread(Context, Start, Size) ->
    Size2 = toolkit:get_proper_size(Start, Context#file_context.file_size, Size),
    End = Start + Size2,
    read_them(Context, Start, End, []).

read_them(Context, Start, End, BinaryList) when Start < End and is_record(Context, file_context) ->
    ChunkIndex = Start div ?CHUNKSIZE,
    Begin = Start rem ?CHUNKSIZE,
    Size = toolkit:get_proper_size(Start, ?CHUNKSIZE, End - Start),
    Now = toolkit:timestamp(),

    {ok, Socket, Context1} = get_socket(ChunkIndex, Begin, Size, Context, Now),
    {ok, Binary} = receive_it(Socket, [], 0, Size),
    
    BinaryList2 = [Binary|BinaryList],
    Start2 = Start + Size,
    Context2 = Context1#file_context{timestamp = toolkit:timestamp()},
    read_them(Context2, Start2, End, BinaryList2);
read_them(Context, _Start, _End, BinaryList) ->
    OrderList = lists:reverse(BinaryList),
    Result = list_to_binary(OrderList),
    {ok, Result, Context}.

receive_it(Socket, BinaryList, Len, Size) when Len < Size ->
    receive
	{tcp, Socket, Binary} ->
	    BinaryList2 = [Binary | BinaryList],
	    Len2 = Len + size(Binary),
	    receive_it(Socket, BinaryList2, Len2, Size);
	{tcp_closed, Socket} ->
	    {error, socket_broken, "data socket broken when receiving data"}
    end;
receive_it(_Socket, BinaryList, _Len, _Size) ->
    OrderList = lists:reverse(BinaryList),
    Result = list_to_binary(OrderList),
    {ok, Result}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%                       open & close
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
open(FileName, Mode) ->
    case gen_server:call(?META_SERVER, {open, FileName, Mode}) of
	{ok, FileID} ->
	    {ok, FileInfo} = gen_server:call(?META_SERVER, {getfileattr, FileName}),
	    {Size, _, _, _, _} = FileInfo,
	    Context = #file_context{file_id = FileID, file_size = Size, pid = self()},
	    {ok, Context};
	{error, Why} ->
	    {error, Why}
    end.

close(Context) ->
    #file_context{socket=Socket, file_id=FileID} = Context,
    case Socket of
	undefined ->
	    void;
	_Any ->
	    gen_tcp:close(Socket)
    end,
    gen_server:call(?META_SERVER, {close, FileID}).

delete(FileName) ->
    {ok, _} = gen_server:call(?META_SERVER, {delete, FileName}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%   tools
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_chunk_info(FileID, ChunkIndex) ->
    gen_server:call(?META_SERVER, {locatechunk, FileID, ChunkIndex}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Context.file_id && file_size be defined.
%% generate a new file_context including a new socket
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
generate_new(ChunkIndex, Begin, Size, Context) ->
    {ok, ChunkID, Nodelist} = get_chunk_info(Context#file_context.file_id, ChunkIndex),
    [H|_T] = Nodelist, 
    {ok, IP, Port} = gen_server:call(H, {readchunk, ChunkID, Begin, Size}), 
    {ok, Socket} = gen_tcp:connect(IP, Port, ?INET_OP),
    NewContext = Context#file_context{chunk_index = ChunkIndex,
	                                  chunk_id = ChunkID,
					  nodelist = Nodelist,
					  socket = Socket,
					  timestamp = toolkit:timestamp()},

    {ok, Socket, NewContext}.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% get a socket either from cache or new,
%%% and update the file_context.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_socket(ChunkIndex, Begin, Size, Context, _Now) when Context#file_context.socket =:= undefined ->
    generate_new(ChunkIndex, Begin, Size, Context);
get_socket(ChunkIndex, Begin, Size, Context, _Now) when Context#file_context.chunk_index =/= ChunkIndex ->
    gen_tcp:close(Context#file_context.socket),
    generate_new(ChunkIndex, Begin, Size, Context);
get_socket(ChunkIndex, Begin, Size, Context, Now) when (Now - Context#file_context.timestamp >= 15) ->
    gen_tcp:close(Context#file_context.socket),
    generate_new(ChunkIndex, Begin, Size, Context);
get_socket(ChunkIndex, Begin, Size, Context, _Now) ->
    Socket = Context#file_context.socket,
    ChunkID = Context#file_context.chunk_id,
    Req = {readchunk, ChunkID, Begin, Size},
    activate_socket(Socket, Req, ChunkIndex, Context).

activate_socket(Socket, Req, ChunkIndex, Context) ->
    gen_tcp:send(Socket, term_to_binary(Req)),
    receive
	{tcp, Socket, {ok, readchunk}} ->
	    _Result = {ok, Socket, Context}
    after
	10000 ->
	    gen_tcp:close(Socket),
	    {_, _, Begin, Size} = Req,
	    {ok, NewSocket, NewContext} = generate_new(ChunkIndex, Begin, Size, Context),
	    _Result = {ok, NewSocket, NewContext}
    end.
