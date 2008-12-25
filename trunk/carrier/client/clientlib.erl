%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : 
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(clientlib).
-include_lib("kernel/include/file.hrl").
-include("../include/egfs.hrl").
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

do_pread(FileID, Start, Length) ->
    %{ok, FileID} = do_open(FileName, r),
    Start_addr = Start,
    End_addr = Start + Length,
    readchunks(FileID, {Start_addr, End_addr}).
    %{ok}.

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
	    {ok, "you close the file!"};
        {error, Why} -> 
	    ?DEBUG("[Client]:Close file error~p~n",[Why]),
	    {error,Why}
    end.

%compute the chunk index
do_pwrite(FileID, Location, Bytes) ->
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
	    %loop_controllink_receive(Host, Socket, Bytes),
	    gen_tcp:send(Socket, term_to_binary({finish, "info"}))
	    %gen_tcp:close(Socket)
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


readchunks(FileID, {Start_addr, End_addr}) ->
    Start_ChunkIndex = Start_addr div ?CHUNKSIZE,
    End_ChunkIndex = End_addr div ?CHUNKSIZE,
    Start = Start_addr rem ?CHUNKSIZE,
    End = End_addr rem ?CHUNKSIZE,
    case file:open("/tmp/temp.txt",[read]) of
	{ok,_} ->
	    ?DEBUG("[Client]:/tmp has the file. Please rename it or remove it.~n",[]);
	{error, enoent} ->	    
	    case file:open("/tmp/temp.txt", [raw, append, binary]) of
		{ok, Hdl} ->
		    loop_read_chunk(FileID, Start_ChunkIndex, End_ChunkIndex, Start, End, Hdl),
		    file:close(Hdl),
		    {ok};
		{error, Why} ->
		    ?DEBUG("[Client]:Open file error:~p", [Why]),
		    {error, Why}
	    end;
	{error, enospc} ->
	    ?DEBUG("[Client]:There is no space on the device!!",[])
    end.
    

loop_read_chunk(FileID, ChunkIndex, End_ChunkIndex, Start, End, Hdl) when End_ChunkIndex >=ChunkIndex ->
    case  End_ChunkIndex-ChunkIndex  of
        0  ->
            Size = End - Start;
        _Any ->
            Size = ?CHUNKSIZE - Start
    end,

    ?DEBUG("[Client]:ChunkIndex:~p, Size is:~p", [ChunkIndex,Size]),
    {ok, ChunkID,_NodeList} = gen_server:call(?METAGENSERVER, {locatechunk, FileID, ChunkIndex}),
    %NodeID=hd(NodeList),
    %{ok,DataServer}=inet:getaddr(NodeID,inet4),
    Result = gen_server:call(?DATAGENSERVER, {readchunk, ChunkID, Start, Size}),
    {ok, Host, Port} = Result,
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    process_flag(trap_exit, true),
    receive
        {tcp, Socket, Binary} ->
            {ok, Data_Port} = binary_to_term(Binary),
            %% spawn_link(fun() -> receive_data(Host, Data_Port) end);
            receive_data(Host, Data_Port, Hdl),
            Index1 = ChunkIndex + 1,
            loop_read_chunk(FileID, Index1, End_ChunkIndex, 0, End, Hdl);
        {tcp_close, Socket} ->
            ?DEBUG("[Client]:read file closed~n",[]),
	    void
    end;
loop_read_chunk(_, _, _, _, _,_) ->
    ?DEBUG("[Client]:read file closed~n",[]),
    void.
  
receive_data(Host, Port, Hdl) ->
     %?DEBUG("data port:~p~n", [Port]),
     {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),   
     loop_recv(DataSocket, Hdl).  

loop_recv(DataSocket, Hdl) ->
    receive
	{tcp, DataSocket, Data} ->
	    write_data(Data, Hdl),
	    loop_recv(DataSocket, Hdl);
	{tcp_closed, DataSocket} ->
	    ?DEBUG("-->[Client]:read chunk over!~n",[]);
	{client_close, _Why} ->
	    %?DEBUG("[Client]:client close the datasocket~n",[]),
	    gen_tcp:close(DataSocket)
    end.

write_data(Data, Hdl) ->
    file:write(Hdl, Data).
