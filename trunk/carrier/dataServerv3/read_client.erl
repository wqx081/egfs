-module(read_client).
-include("../include/egfs.hrl").
-include("data_server.hrl").
-import(toolkit, [get_proper_size/3,
		  timestamp/0]).
-export([pread/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%Context = {FileID, FileSize, 
%%		 ChunkIndex, ChunkID, 
%%		 Nodelist,
%%		 Socket, Time, Pid}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
pread(Context, Start, Size) ->
    Size2 = toolkit:get_proper_size(Start, Context#read_context.file_size, Size),
    End = Start + Size2,
    read_them(Context, Start, End, []).

read_them(Context, Start, End, BinaryList) when Start < End and is_record(Context, read_context) ->
    ChunkIndex = Start div ?CHUNKSIZE,
    Begin = Start rem ?CHUNKSIZE,
    Size = toolkit:get_proper_size(Start, ?CHUNKSIZE, End - Start),
    Now = toolkit:timestamp(),

    {ok, Socket, Context1} = get_socket(ChunkIndex, Begin, Size, Context, Now),
    {ok, Binary} = receive_it(Socket, [], 0, Size),
    
    BinaryList2 = [Binary|BinaryList],
    Start2 = Start + Size,
    Context2 = Context1#read_context{timestamp = toolkit:timestamp()},
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%   tools
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_chunk_info(FileID, ChunkIndex) ->
    gen_server:call(?META_SERVER, {locatechunk, FileID, ChunkIndex}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Context.file_id && file_size be defined.
%% generate a new read_context including a new socket
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
generate_new(ChunkIndex, Begin, Size, Context) ->
    {ok, ChunkID, Nodelist} = get_chunk_info(Context#read_context.file_id, ChunkIndex),
    [H|_T] = Context#read_context.nodelist, 
    {ok, IP, Port} = gen_server:call(H, {readchunk, ChunkID, Begin, Size}), 
    {ok, Socket} = gen_tcp:connect(IP, Port, ?INET_OP),
    NewContext = Context#read_context{chunk_index = ChunkIndex,
	                                  chunk_id = ChunkID,
					  nodelist = Nodelist,
					  socket = Socket,
					  timestamp = toolkit:timestamp()},

    {ok, Socket, NewContext}.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% get a socket either from cache or new,
%%% and update the read_context.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_socket(ChunkIndex, Begin, Size, Context, _Now) when Context#read_context.socket =:= undefined ->
    generate_new(ChunkIndex, Begin, Size, Context);
get_socket(ChunkIndex, Begin, Size, Context, _Now) when Context#read_context.chunk_index =/= ChunkIndex ->
    gen_tcp:close(Context#read_context.socket),
    generate_new(ChunkIndex, Begin, Size, Context);
get_socket(ChunkIndex, Begin, Size, Context, Now) when (Now - Context#read_context.timestamp >= 15) ->
    gen_tcp:close(Context#read_context.socket),
    generate_new(ChunkIndex, Begin, Size, Context);
get_socket(ChunkIndex, Begin, Size, Context, _Now) ->
    Socket = Context#read_context.socket,
    ChunkID = Context#read_context.chunk_id,
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
