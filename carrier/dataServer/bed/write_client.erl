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
    ok = send_it(Socket, Part, 0, Size),

    Start2 = Start + Size,
    WriteContext2 = WrtieContext1#write_context{timestamp = toolkit:timestamp()},
    write_them(WriteContext2, Start2, End, Left);
write_them(WriteContext, _Start, _End, _Binary) ->
    {ok, WriteContext}.
    
send_it(Socket, Binary) when size(Binary) > 0 ->
    Size = get_proper_size(0, size(Binary), ?STRIP_SIZE),
    {Part, Left} = split_binary(Binary, Size),
    
    gen_tcp:send(Socket, Part),
    send_it(Socket, Left);
send_it(_Socket, _Binary) ->
    ok.
    
