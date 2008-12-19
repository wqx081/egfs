-module(shout_server).
-include_lib("kernel/include/file.hrl").
-include("../include/egfs.hrl").
-export([start/0]).

-define(STRIP_SIZE, 8192).
-define(DEF_PORT, 9999).
-define(TO_SEND, "hello.rmvb").

start() ->
    spawn(fun() -> 
		start_parallel_server(?DEF_PORT),
		lib_misc:sleep(infinity)
		end).

start_parallel_server(Port) ->
    {ok, Listen} = gen_tcp:listen(Port, [binary, {packet, 2}, 
						 {reuseaddr, true},
						 {active, true}]),
    register(shout_server, self()),
    spawn(fun() -> par_connect(Listen) end).

par_connect(Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    spawn(fun() -> par_connect(Listen) end),
    inet:setopts(Socket, [binary, {packet, 2}, {nodelay, true}, {active, true}]),

    get_request(Socket).

get_request(Socket) ->
    receive
	{tcp, Socket, Bin} ->
	    %%TODO: verify that we have receive the full Req first.
	    Req = binary_to_term(Bin),
	    ?DEBUG("[shoutServer]: receive req ~p~n", [Req]),

	    case Req of
		{read, ChunkID, Begin, Size} ->
		    read_response(Socket, ChunkID, Begin, Size);
		{write, ChunkID} ->
		    write_response(Socket, ChunkID);
		_Any ->
		    ?DEBUG("[shoutServer]: unkown req ~p~n", [Req])
	    end;

	{tcp_close, Socket} ->
	    ?DEBUG("[shoutServer]: control connect closed by client~n", []);
	_Unkown ->
	    ?DEBUG("[shoutServer]: something unkown to me~n", [])
    end.

read_response(Socket, _ChunkID, Begin, Size) ->
    {ok, FileSize} = get_file_size(?TO_SEND),

    if 
	(Begin >= FileSize) orelse (Size =< 0) ->
	    gen_tcp:send(Socket, term_to_binary({error, "invalid read boundary"})),
	    ?DEBUG("[shoutServer]: read bundary invalid ~p~n", [Begin]);
	true ->
	    if 
		Begin + Size > FileSize ->
		    End = FileSize;
		true ->
		    End = Begin + Size
	    end,

	    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
	    Reply = inet:port(ListenData), %% Reply = {ok, Port}
	    gen_tcp:send(Socket, term_to_binary(Reply)),

	    {ok, SocketData} = gen_tcp:accept(ListenData),
	    {ok, Hdl} = file:open(?TO_SEND, [binary, raw, read]),

	    send_it(SocketData, Hdl, Begin, End)
    end.

send_it(SocketData, Hdl, Begin, End) when Begin < End ->
    {ok, Binary} = file:pread(Hdl, Begin, ?STRIP_SIZE),
    gen_tcp:send(SocketData, Binary),
    Begin2 = Begin + ?STRIP_SIZE,
    send_it(SocketData, Hdl, Begin2, End);
send_it(_, _, _, _) ->
    void.

get_file_size(File) ->
    case file:read_file_info(File) of
	{ok, Facts} ->
	    {ok, Facts#file_info.size};
	_ ->
	    error
    end.


write_response(_Socket, _ChunkID) ->
    ?DEBUG("[shoutServer]: response for write~n", []),
    ?DEBUG("[shoutServer]: response for write has not been implemented yet.~n", []).
