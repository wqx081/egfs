-module(read_client).
-include("data_server.hrl").
-import(toolkit, [get_proper_size/3,
		  timestamp/0]).
-export([pwrite/3]).

pwrite(WriteContext, Start, Binary) ->
    End = Start + size(Binary),
    write_them(WriteContext, Start, End, Binary).

write_them(WrtieContext, Start, End, Binary) when Start < End and is_record(WriteContext, write_context) ->
    ChunkIndex = Start div ?CHUNKSIZE,
    Begin = Start rem ?CHUNKSIZ,
    Size = toolkit:get_proper_size(Start, ?CHUNKSIZE - Begin, End - Start),

    {ok, Socket, WrtieContext1} = get_socket(ChunkIndex, Begin, Size, WriteContext),
    {Part, Left} = split_binary(Binary, Size),
    ok = send_it(Socket, Part),

    Start2 = Start + Size,
    WriteContext2 = WrtieContext1#write_context{timestamp = toolkit:timestamp()},
    write_them(WriteContext2, Start2, End, Left);
write_them(WriteContext, _Start, _End, _Binary) ->
    {ok, WriteContext}.
    
send_it(Socket, Binary) when size(Binary) > 0 ->
    Size = toolkit:get_proper_size(0, size(Binary), ?STRIP_SIZE),
    {Part, Left} = split_binary(Binary, Size),
    gen_tcp:send(Socket, Part),
    send_it(Socket, Left);
send_it(_Socket, _Binary) ->
    ok.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%		    tools
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_chunk_info(FileID, ChunkIndex) ->
    Reply = gen_server:call(?META_SERVER, {locatechunk, FileID, ChunkIndex}),
    case Reply of
	{ok, _ChunkID, _Nodelist} ->
	    Reply;
	{error, _} ->
	    _Result = gen_server:call(?META_SERVER, {allocatechunk, FileID});
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% WriteContext.file_id && file_size be defined.
%% generate a new write_context including a new socket
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
generate_new(ChunkIndex, WriteContext) ->
    FileID = WriteContext#write_context.file_id,
    {ok, ChunkID, Nodelist} = get_chunk_info(FileID, ChunkIndex),
    [H|_T] = WriteContext.Nodelist, 
    {ok, IP, Port} = gen_server:call(H, {writechunk, FileID, ChunkID, Begin, Size}), 
    {ok, Socket} = gen_tcp:connect(IP, Port, ?INET_OP),
    NewContext = WriteContext#write_context{chunk_index = ChunkIndex,
					    chunk_id = ChunkID,
					    nodelist = Nodelist,
					    socket = Socket,
					    timestamp = toolkit:timestamp()},

    {ok, Socket, NewContext}.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% get a socket either from cache or new,
%%% and update the write_context.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_socket(ChunkIndex, Begin, Size, WriteContext) when WriteContext#write_context.socket =:= undefined ->
    generate_new(ChunkIndex, Begin, Size, WriteContext);
get_socket(ChunkIndex, Begin, Size, WriteContext) when WriteContext#write_context.chunk_index <> ChunkIndex ->
    gen_tcp:close(WriteContext#write_context.socket),
    generate_new(ChunkIndex, Begin, Size, WriteContext);
get_socket(ChunkIndex, Begin, Size, WriteContext) when (toolkit:timestamp() - WriteContext#write_context.timestamp) >= 15  ->
    gen_tcp:close(WriteContext#write_context.socket),
    generate_new(ChunkIndex, Begin, Size, WriteContext);
get_socket(ChunkIndex, Begin, Size, WriteContext) ->
    Socket = WriteContext#write_context.socket,
    ChunkID = WriteContext#write_context.chunk_id,
    [_H | Nodelist] = WriteContext#write_context.nodelist,
    Req = {writechunk, FileID, ChunkID, Begin, Size, Nodelist},
    activate_socket(Socket, Req, ChunkIndex).

activate_socket(Socket, Req, ChunkIndex) ->
    gen_tcp:send(Socket, term_to_binary(Req)),
    receive
	{tcp, Socket, {ok, readchunk}} ->
	    _Result = {ok, Socket, WriteContext};
    after
	10000 ->
	    gen_tcp:close(Socket),
	    {ok, NewSocket, NewContext} = generate_new(ChunkIndex, Begin, Size, WriteContext),
	    _Result = {ok, NewSocket, NewContext},
    end.
