-module(metaserver).
-compile(export_all).

start_parallel_server() ->
    {ok, Listen} = gen_tcp:listen(9999, [binary, 
					 {packet, 2}, 
					 {active, true},
					 {reuseaddr, true}]),
   % register(?MODULE, self()),
    spawn(fun() -> par_connect(Listen) end).


par_connect(Listen) ->
    %% io:format("new listen process:<<~p>>~n", self()),
    {ok, Socket} = gen_tcp:accept(Listen),
    spawn(fun() -> par_connect(Listen) end),
    loop(Socket).  %handle socket
    % handle_client_req(Socket).

loop(Socket) ->
    % process_flag(trap_exit, true),

    receive
	{tcp, Socket, Bin} ->
	    Req = binary_to_term(Bin),
	    io:format("Server received req:~p~n", [Req]),

	    case Req of
		{open, FileName, _Mode} ->
		    do_open( FileName,Mode,Socket);
		{allocate, FileID} ->
		    do_allocate_chunk(FileID,Socket);
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



%"model" methods
% write step 1: open file
do_open(Filename, Modes, ClientID)->
    % Modes 
    case Modes of
        r-> do_read_open(Filename, ClientID);
        w-> do_write_open(Filename, ClientID);
        a-> do_append_open(Filename, ClientID);
        _-> {error, "unkown open mode"}
    end.
 
do_read_open(Filename, ClientID)->
    % mock return
    %TODO: Table: clientinfo  ,     
    % insert a record
    {ok, <<16#ff00ff00ff00ff00:64>>}.

do_write_open(Filename, ClientID)->
    % mock return
    %TODO: Table: clientinfo  ,
    %TODO: Table: filesession {fileid, client} ,mode =w
    % insert a record
    {ok, <<16#ff00ff00ff00ff00:64>>}.

do_append_open(Filename, ClientID)->
    % mock return
    %TODO: Table: clientinfo  ,
    %TODO: Table: filesession {fileid, client} ,mode =w
    % insert a record
    {ok, <<16#ff00ff00ff00ff00:64>>}.

% write step 2: allocate chunk
do_allocate_chunk(FileID, ClientID)->
    % look up "filesession" for client open Modes
    Modes = look_up_filesession(FileID, ClientID), %do it inside mnesia
    % allocate data chunk
    case Modes of
        w-> get_last_chunk(FileID);
        a-> get_last_chunk(FileID)
    end.



% unused function  a
 get_first_chunk(FileID)->
    % mock return
    {ok, <<16#ff00ff00ff00ff00:64>>, [nodeip1, nodeip2, nodeip3]}.
    
get_last_chunk(FileID)->
    % mock return
    {ok, <<16#ff00ff00ff00ff00:64>>, [nodeip1, nodeip2, nodeip3]}.

% write step 3: register chunk
do_register_chunk(FileID, ChunkID, ChunkUsedSize, NodeList)->
    % register chunk
    % do nothing in Milestone ONE
    {ok, {}}.

% write step 4: close file
do_close(FileID, ClientID)->
    % delete client from filesession table
    {}.

% read step 1: open file == wirte step1
% read step 2: get chunk for further reading
do_get_chunk(FileID, ChunkIdx)->
    % mock return
{ok,  <<16#ff00ff00ff00ff00:64>>, [nodeip1, nodeip2, nodeip3]}.

% read step 4: close file == write step 4
