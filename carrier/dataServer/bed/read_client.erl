-module(read_client).
-include("data_server.hrl").
-import(toolkit, [get_proper_size/3,
		  timestamp/0]).
-export([pread/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%ReadContext = {FileID, FileSize, 
%%		 ChunkIndex, ChunkID, 
%%		 Nodelist,
%%		 Socket, Time, Pid}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
pread(ReadContext, Start, Size) ->
    Size2 = toolkit:get_proper_size(Start, ReadContext#read_context.file_size, Size),
    End = Start + Size2,
    read_them(ReadContext, Start, End, []).

read_them(ReadContext, Start, End, BinaryList) when Start < End and is_record(ReadContext, read_context) ->
    ChunkIndex = Start div ?CHUNKSIZE,
    Begin = Start rem ?CHUNKSIZ,
    Size = toolkit:get_proper_size(Start, ?CHUNKSIZE, End - Start),

    {ok, Socket, ReadContext1} = get_socket(ChunkIndex, Begin, Size, ReadContext),
    {ok, Binary} = receive_it(Socket, [], 0, Size),
    
    BinaryList2 = [Binary|BinaryList],
    Start2 = Start + Size,
    ReadContext2 = ReadContext1#read_context{timestamp = toolkit:timestamp()},
    read_them(ReadContext2, Start2, End, BinaryList2);
read_them(ReadContext, _Start, _End, BinaryList) ->
    OrderList = lists:reverse(BinaryList),
    Result = list_to_binary(OrderList),
    {ok, Result, ReadContext}.

receive_it(Socket, BinaryList, Len, Size) when Len < Size ->
    receive
	{tcp, Socket, Binary} ->
	    BinaryList2 = [Binary | BinaryList],
	    Len2 = Len + size(Binary),
	    receive_it(Socket, BinaryList2, Len2, Size);
	{tcp_closed, Socket} ->
	    {error, socket_broken, "data socket broken when receiving data"}
    end;
receive_it(Socket, BinaryList, Len, Size) ->
    OrderList = lists:reverse(BinaryList),
    Result = list_to_binary(OrderList),
    {ok, Result}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%   tools
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_chunk_info(FileID, ChunkIndex) ->
    io:format("[~p, ~p] get_chunk_info not implemented!~n", [?MODULE, ?LINE]),
    {ok, ChunkID, Nodelist}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ReadContext.file_id && file_size be defined.
%% generate a new read_context including a new socket
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
generate_new(ChunkIndex, ReadContext) ->
    {ok, ChunkID, Nodelist} = get_chunk_info(ReadContext#file_id, ChunkIndex),
    [H|_T] = ReadContext.Nodelist, 
    {ok, IP, Port} = gen_server:call(H, {readchunk, ChunkID, Begin, Size}), 
    {ok, Socket} = gen_tcp:connect(IP, Port, ?INET_OP),
    NewContext = ReadContext#read_context{chunk_index = ChunkIndex,
	                                  chunk_id = ChunkID,
					  nodelist = Nodelist,
					  socket = Socket,
					  timestamp = toolkit:timestamp()},

    {ok, Socket, NewContext}.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% get a socket either from cache or new,
%%% and update the read_context.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_socket(ChunkIndex, Begin, Size, ReadContext) when ReadContext#read_context.socket =:= undefined ->
    generate_new(ChunkIndex, Begin, Size, ReadContext);
get_socket(ChunkIndex, Begin, Size, ReadContext) when ReadContext#read_context.chunk_index <> ChunkIndex ->
    gen_tcp:close(ReadContext#read_context.socket),
    generate_new(ChunkIndex, Begin, Size, ReadContext);
get_socket(ChunkIndex, Begin, Size, ReadContext) when (toolkit:timestamp() - ReadContext#read_context.timestamp) >= 15  ->
    gen_tcp:close(ReadContext#read_context.socket),
    generate_new(ChunkIndex, Begin, Size, ReadContext);
get_socket(ChunkIndex, Begin, Size, ReadContext) ->
    Socket = ReadContext#read_context.socket,
    ChunkID = ReadContext#read_context.chunk_id,
    Req = {readchunk, ChunkID, Begin, Size},
    gen_tcp:send(Socket, term_to_binary(Req)),

    receive
	{tcp, Socket, {ok, readchunk}} ->
	    Result = {ok, Socket, ReadContext};
    after
	10000 ->
	    gen_tcp:close(Socket),
	    {ok, NewSocket, NewContext} = generate_new(ChunkIndex, Begin, Size, ReadContext),
	    Result = {ok, NewSocket, NewContext},
    end,

    Result.
