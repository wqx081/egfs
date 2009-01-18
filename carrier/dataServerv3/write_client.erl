-module(write_client).
-include("../include/egfs.hrl").
-include("data_server.hrl").
-import(toolkit, [get_proper_size/3,
		  timestamp/0]).
-export([pwrite/3]).

pwrite(Context, Start, Binary) ->
    End = Start + size(Binary),
    write_them(Context, Start, End, Binary).

write_them(Context, Start, End, Binary) when Start < End and is_record(Context, write_context) ->
    ChunkIndex = Start div ?CHUNKSIZE,
    Begin = Start rem ?CHUNKSIZE,
    Size = toolkit:get_proper_size(Start, ?CHUNKSIZE - Begin, End - Start),
    Now = toolkit:timestamp(),

    {ok, Socket, WrtieContext1} = get_socket(ChunkIndex, Begin, Size, Context, Now),
    {Part, Left} = split_binary(Binary, Size),
    ok = send_it(Socket, Part),

    Start2 = Start + Size,
    Context2 = WrtieContext1#write_context{timestamp = toolkit:timestamp()},
    write_them(Context2, Start2, End, Left);
write_them(Context, _Start, _End, _Binary) ->
    {ok, Context}.
    
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
	    _Result = gen_server:call(?META_SERVER, {allocatechunk, FileID})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Context.file_id && file_size be defined.
%% generate a new write_context including a new socket
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
generate_new(ChunkIndex, Begin, Size, Context) ->
    FileID = Context#write_context.file_id,
    {ok, ChunkID, Nodelist} = get_chunk_info(FileID, ChunkIndex),
    [H|T] = Context#write_context.nodelist, 
    {ok, IP, Port} = gen_server:call(H, {writechunk, FileID, ChunkID, Begin, Size, T}), 
    {ok, Socket} = gen_tcp:connect(IP, Port, ?INET_OP),
    NewContext = Context#write_context{chunk_index = ChunkIndex,
					    chunk_id = ChunkID,
					    nodelist = Nodelist,
					    socket = Socket,
					    timestamp = toolkit:timestamp()},

    {ok, Socket, NewContext}.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% get a socket either from cache or new,
%%% and update the write_context.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_socket(ChunkIndex, Begin, Size, Context, _Now) when Context#write_context.socket =:= undefined ->
    generate_new(ChunkIndex, Begin, Size, Context);
get_socket(ChunkIndex, Begin, Size, Context, _Now) when Context#write_context.chunk_index =/= ChunkIndex ->
    gen_tcp:close(Context#write_context.socket),
    generate_new(ChunkIndex, Begin, Size, Context);
get_socket(ChunkIndex, Begin, Size, Context, Now) when (Now - Context#write_context.timestamp) >= 15  ->
    gen_tcp:close(Context#write_context.socket),
    generate_new(ChunkIndex, Begin, Size, Context);
get_socket(ChunkIndex, Begin, Size, Context, _Now) ->
    FileID = Context#write_context.file_id,
    ChunkID = Context#write_context.chunk_id,
    Socket = Context#write_context.socket,
    [_H | Nodelist] = Context#write_context.nodelist,
    Req = {writechunk, FileID, ChunkID, Begin, Size, Nodelist},
    activate_socket(Socket, Req, ChunkIndex, Context).

activate_socket(Socket, Req, ChunkIndex, Context) ->
    gen_tcp:send(Socket, term_to_binary(Req)),
    receive
	{tcp, Socket, {ok, _}} ->
	    _Result = {ok, Socket, Context}
    after
	10000 ->
	    gen_tcp:close(Socket),
	    {_, _, _, Begin, Size, _} = Req,
	    {ok, NewSocket, NewContext} = generate_new(ChunkIndex, Begin, Size, Context),
	    _Result = {ok, NewSocket, NewContext}
    end.
