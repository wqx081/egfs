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
-export([do_open/2, do_pread/3, 
	 do_pwrite/3, do_delete/1, 
	 do_close/1, get_file_name/1]).
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
    Start_addr = Start,
    End_addr = Start + Length,
    read_them(FileID, {Start_addr, End_addr}).
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
    ?DEBUG("[Client]:ChunkID IS ~p",[ChunkID]),
    {ok, Host, Port} = gen_server:call(?DATAGENSERVER, {writechunk, FileID, ChunkIndex, ChunkID, NodeList}),
    ?DEBUG("[Client]:Host is ~p,Port is ~p",[Host, Port]),
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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%          tools 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_file_name(FileID) ->
    <<Int1:64>> = FileID,
    lists:append(["/tmp/", integer_to_list(Int1)]). 

delete_file(FileID) ->
    FileName = get_file_name(FileID), 
    file:delete(FileName).

get_file_handle(write, FileID) ->
    FileName = get_file_name(FileID), 
    case file:open(FileName, [raw, append, binary]) of
	{ok, Hdl} ->	
	    {ok, Hdl};
	{error, Why} ->
	    ?DEBUG("[Client]:Open file error:~p", [Why]),
	    void
    end.

get_chunk_info(FileID, ChunkIndex) -> 
    gen_server:call(?METAGENSERVER, {locatechunk, FileID, ChunkIndex}).
	    
get_new_chunk(FileID, _ChunkIndex) ->
    gen_server:call(?METAGENSERVER, {allocatechunk, FileID}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%          read
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
read_them(FileID, {Start, End}) ->
    ChunkIndex = Start div ?CHUNKSIZE, 
    delete_file(FileID),
    loop_read_chunks(FileID, ChunkIndex, Start, End).

loop_read_chunks(FileID, ChunkIndex, Start, End) when Start < End ->
    {ok, ChunkID, _Nodelist} = get_chunk_info(FileID, ChunkIndex),
    Begin = Start rem ?CHUNKSIZE,
    Size1 = ?CHUNKSIZE - Begin,

    if 
	Size1 + Start =< End ->
	    Size = Size1;
	true ->
	    Size = End - Start
    end,

    read_a_chunk(FileID, ChunkIndex, ChunkID, Begin, Size),
    ChunkIndex2 = ChunkIndex + 1,
    Start2 = Start + Size,
    loop_read_chunks(FileID, ChunkIndex2, Start2, End);
loop_read_chunks(_, _, _, _) ->
    ?DEBUG("all chunks read over!~n", []).

read_a_chunk(FileID, _ChunkInedx, ChunkID, Begin, Size) when Size =< ?CHUNKSIZE ->
    {ok, Host, Port} = gen_server:call(?DATAGENSERVER, {readchunk, ChunkID, Begin, Size}),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    Parent = self(),
    receive
        {tcp, Socket, Binary} ->
            {ok, Data_Port} = binary_to_term(Binary),
	    process_flag(trap_exit, true),
	    Child = spawn_link(fun() -> receive_it(Parent, Host, Data_Port, FileID) end),
	    loop_receive_ctrl(Socket, Child);  
	{tcp_close, Socket} ->
            ?DEBUG("[Client]:read file closed~n",[]),
	    void
    end;    
read_a_chunk(_, _, _, _,_) ->
    void.

loop_receive_ctrl(Socket, Child) ->
    receive
	{finish, Child, _Len} ->	
	    ?DEBUG("[Client, ~p]:recive packets finished~n",[?LINE]);
        {tcp, Socket, Binary} -> 
            Term = binary_to_term(Binary),
	    case Term of
		{stop, Why} ->	    	    
		    ?DEBUG("[Client, ~p]:stop send from dataserver~n",[?LINE]),
		    Child ! {stop, self(), Why};
		    %% exit(Child, kill);
		{finish, _, _Len} ->
		    ?DEBUG("[Client, ~p]:finish send from dataserver~n",[?LINE]),
		    void;
		_Any ->
		    loop_receive_ctrl(Socket, Child)
	    end;
	{error, Child, Why} ->
	    ?DEBUG("[Client]:data receive socket error!~p~n",[Why]);
	_Any ->
	    ?DEBUG("[Client, ~p]:unknow messege!~n",[?LINE]),
	    loop_receive_ctrl(Socket, Child)
    end.

receive_it(Parent, Host, Data_Port, FileID) ->
    {ok, DataSocket} = gen_tcp:connect(Host, Data_Port, [binary, {packet, 2}, {active, true}]),
    {ok, Hdl} = get_file_handle(write, FileID),
    loop_recv_packet(Parent, DataSocket, Hdl, 0),
    file:close(Hdl).
    
loop_recv_packet(Parent, DataSocket, Hdl, Len) ->
    receive
	{tcp, DataSocket, Data} ->
	    write_data(Data, Hdl),
	    Len2 = Len + size(Data),
	    loop_recv_packet(Parent, DataSocket, Hdl, Len2);
	{tcp_closed, DataSocket} ->
	    ?DEBUG("[Client]:read chunk over! ~p~n",[?LINE]),
	    Parent ! {finish, self(), Len};
	{stop, Parent, _Why} ->
	    ?DEBUG("[Client]:client close the datasocket~n",[]),
	    gen_tcp:close(DataSocket),
	    ?DEBUG("[Client]:client close",[]);
	_Any ->	    
	    ?DEBUG("[Client]:go any ~p~n",[?LINE])
    end.

write_data(Data, Hdl) ->
    file:write(Hdl, Data).
