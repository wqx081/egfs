%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : 
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(clientlib).
-include_lib("kernel/include/file.hrl").
-include("egfs.hrl").
-export([do_open/2, do_pread/3, do_pwrite/3, do_delete/1, do_close/1]).
-compile(export_all).
-define(STRIP_SIZE, 8192).   % 8*1024

do_open(FileName, Mode) ->
    case gen_server:call(?METAGENSERVER, {open, FileName, Mode}) of
        {ok, FileID} ->
            FileID;
        {error, Why} ->
			?DEBUG("[Client]:Open file error:~p~n",[Why])
    end.

do_pread(_FileName, _Start, _Length) ->
	{ok}.

do_delete(FileName) -> 
    case gen_server:call(?METAGENSERVER, {delete, FileName}) of
        {ok,_} -> 
			{ok};
        {error, Why} -> 
			?DEBUG("[Client]:Delete file error~p~n",[Why]),
			{error,Why}
    end.

do_close(FileID) ->
	?DEBUG("[Client]:Send Close Msg~n",[]), 
    case gen_server:call(?METAGENSERVER, {close, FileID}) of
        {ok,_} ->
			?DEBUG("[Client]:Close file ok~n",[]), 
			{ok};
        {error, Why} -> 
			?DEBUG("[Client]:Close file error~p~n",[Why]),
			{error,Why}
    end.

do_pwrite(FileID, Location, Bytes) ->
	%compute the chunk index
    case Location rem ?CHUNKSIZE of
        0 -> 
			ChunkIndex = Location div ?CHUNKSIZE;
        _Any -> 
			ChunkIndex = Location div ?CHUNKSIZE + 1
    end,
	Begin = Location rem ?CHUNKSIZE,
    Size = size(Bytes),
	?DEBUG("[Client]:do_pwrite() Begin:~p Size:~p~n",[Begin,Size]),
	
    case (Begin + Size) > ?CHUNKSIZE of
		true->
			W_Size = ?CHUNKSIZE - Begin,

			{X,Y} = split_binary(Bytes, W_Size),
			do_pwrite_it(FileID, ChunkIndex, X),	
			do_pwrite(FileID, ChunkIndex * ?CHUNKSIZE, Y);

%			Int1= W_Size*8,
%			Int2= (Size-W_Size)*8,
%			?DEBUG("[Client]:~p~n", [?LINE]),
%			<<X:Int1, Y:Int2>> = Bytes,
%			?DEBUG("[Client]:~p~n", [?LINE]),
%			do_pwrite_it(FileID, ChunkIndex, <<X:Int1>>),
%			do_pwrite(FileID, ChunkIndex * ?CHUNKSIZE, <<Y:Int1>>);
		false ->
			do_pwrite_it(FileID, ChunkIndex, Bytes)
	end.

do_pwrite_it(FileID, ChunkIndex, Bytes) ->		
	%seek the ChunkID based on the FileID and ChunkIndex
	{ok, ChunkID, NodeList} = seek_chunk(FileID, ChunkIndex),	
	%seek the physics host and port based on the FileID, ChunkIndex, and ChunkID 
 	{ok, Host, Port} = gen_server:call(?DATAGENSERVER, {writechunk, FileID, ChunkIndex, ChunkID, NodeList}),
	%create a control link with the host and port.
	{ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
	%loop receive the message from control link.
	loop_controllink_receive(Host, Socket, Bytes).

loop_controllink_receive(Host, Socket, Bytes) ->
	receive
        {tcp, Socket, Binary} ->
            {ok, Data_Port} = binary_to_term(Binary),
			{ok, DataSocket} = gen_tcp:connect(Host, Data_Port, [binary, {packet, 2}, {active, true}]),
			process_flag(trap_exit, true),
			Parent = self(),
			spawn_link(fun() -> createdatalink(Parent, DataSocket, Bytes) end),
            loop_controllink_receive(Host, Socket, Bytes);
        {tcp_close, Socket} ->
            ?DEBUG("[Client] tcp_close, write file closed~n",[]);
		{finish, Child} ->
			?DEBUG("[Client]:write finish ~p~n", [Child]),
			gen_tcp:close(Socket)
    end.

createdatalink(Parent, DataSocket, Bytes) when size(Bytes) > ?STRIP_SIZE   ->
	{X,Y} = split_binary(Bytes, ?STRIP_SIZE),
	gen_tcp:send(DataSocket, X),
	createdatalink(Parent, DataSocket,Y);	
createdatalink(Parent, DataSocket, Bytes) when size(Bytes) > 0->
	gen_tcp:send(DataSocket, Bytes),
	gen_tcp:close(DataSocket),
	Parent ! {finish, self()};
createdatalink(Parent, DataSocket, _Bytes) ->
    gen_tcp:close(DataSocket),
	Parent ! {finish, self()}.

seek_chunk(FileID, ChunkIndex) ->
	case gen_server:call(?METAGENSERVER, {locatechunk, FileID, ChunkIndex}) of
	{ok, C1, N1} ->
		?DEBUG("[Client]:Line=~p locate chunkID=~p, NodeList=~p~n",[?LINE, C1, N1]),
		{ok, C1, N1};	
	{error,_} ->
		case gen_server:call(?METAGENSERVER, {allocatechunk, FileID}) of
		{ok, C2, N2} ->
			?DEBUG("[Client]:Line=~p  allocate chunkID=~p,NodeList=~p~n",[?LINE, C2, N2]),
			{ok, C2, N2};
		{error, Why} ->
			{error, Why}
		end
	end.
