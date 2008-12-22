%% Author: zyb@fit
%% Created: 2008-12-19
%% Description: TODO: Add description to metaC

-module(metaC).


%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([start/0]).
%%
%% API Functions
%%



%%
%% Local Functions
%%



start() ->
    
	Test = 10000,
    
    {ok, Listen} = gen_tcp:listen(9999, [binary, 
					 {packet, 2}, 
					 {active, true},
					 {reuseaddr, true}]),
    register(?MODULE, self()),
    spawn(fun() -> par_connect(Listen) end).

par_connect(Listen) ->
    %% io:format("new listen process:<<~p>>~n", self()),
    {ok, Socket} = gen_tcp:accept(Listen),
    spawn(fun() -> par_connect(Listen) end),
    handle_client_req(Socket).

handle_client_req(Socket) ->
    process_flag(trap_exit, true),

    receive
	{tcp, Socket, Bin} ->
	    Req = binary_to_term(Bin),
	    io:format("Server received req:~p~n", [Req]),

	    case Req of
		{open, FileName, _Mode} ->
		    open_response(Socket, FileName);
		{allocate, FileID} ->
		    allocate_response(Socket, FileID);
		{locate, FileID, ChunkIndex} ->
		    locate_response(Socket, FileID, ChunkIndex);
		_Any ->
		    io:format("Server Unkown req:~p~n", [Req])
	    end;
	{tcp_close, Socket} ->
	    io:format("Control Socket was closed by Client~n");
	_Unknown ->
	    io:format("something unknown to server~n")
    end.
    
    
open_response(Socket, _FileName) ->
    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    {ok, Port} = inet:port(ListenData),
    FileID = 2000,
    gen_tcp:send(Socket, term_to_binary({ok, Port, FileID})),

    {ok, SocketData} = gen_tcp:accept(ListenData),

    gen_tcp:close(SocketData).



allocate_response(Socket, _FileID) ->
    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    {ok, Port} = inet:port(ListenData),
    ChunkID = 20,
    NodeList={1,2,123},
    gen_tcp:send(Socket, term_to_binary({ok, Port, ChunkID, NodeList})),

    {ok, SocketData} = gen_tcp:accept(ListenData),

    gen_tcp:close(SocketData).
    
locate_response(Socket, _FileID, _ChunkIndex)  ->
    {ok, ListenData} = gen_tcp:listen(0, [binary, {packet, 2}, {active, true}]),
    {ok, Port} = inet:port(ListenData),
    ChunkID = 30,
    NodeList={1,2,125},
    gen_tcp:send(Socket, term_to_binary({ok, Port, ChunkID, NodeList})),

    {ok, SocketData} = gen_tcp:accept(ListenData),

    gen_tcp:close(SocketData).