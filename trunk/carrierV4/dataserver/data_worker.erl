-module(data_worker).
-include("../include/header.hrl").
-export([run/3]).

run(MM, ArgC, _ArgS) ->
 	error_logger:info_msg("[~p, ~p]: dataworker ~p running ArgC =~p ~n", [?MODULE, ?LINE, self(),ArgC]),
	case ArgC of
		{write, ChunkID} ->
			{ok, ChunkHdl} = lib_common:get_file_handle({write, ChunkID}),
		    loop_write(MM, ChunkID, ChunkHdl);
		{read, ChunkID}	->
			{ok, ChunkHdl} = lib_common:get_file_handle({read, ChunkID}),
		    loop_read(MM, ChunkHdl);
	    {replica, ChunkID, MD5} ->
			{ok, ChunkHdl} = lib_common:get_file_handle({write, ChunkID}),
			loop_replica(MM, ChunkID, MD5, ChunkHdl)
	end.


loop_write(MM, ChunkID, ChunkHdl) ->
    receive
	{chan, MM, {write, Bytes}} ->
		%error_logger:info_msg("[~p, ~p]: write ~p ~n", [?MODULE, ?LINE, Bytes]),
		R = file:write(ChunkHdl, Bytes),
	    MM ! {send, R}, 
	    loop_write(MM, ChunkID, ChunkHdl);
	{chan_closed, MM} ->
		{ok, FileName} 	= lib_common:get_file_name(ChunkID),
		{ok, MD5}		= lib_md5:file(FileName),
		data_db:add_chunkmeta_item(ChunkID, MD5),		
		file:close(ChunkHdl),
		error_logger:info_msg("[~p, ~p]: dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.

loop_read(MM, ChunkHdl) ->
    receive
	{chan, MM, {read, Start, Length}} ->
		R = file:pread(ChunkHdl, Start, Length),
		%error_logger:info_msg("[~p, ~p]: send ~p ~n", [?MODULE, ?LINE, R]),		
	    MM ! {send, R},
	    loop_read(MM, ChunkHdl);
	{chan_closed, MM} ->
		file:close(ChunkHdl),
		error_logger:info_msg("[~p, ~p]: dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.

loop_replica(MM, ChunkID, MD5, ChunkHdl) ->
    receive
	{chan, MM, {replica, Bytes}} ->
		%error_logger:info_msg("[~p, ~p]: replica ~p ~n", [?MODULE, ?LINE, Bytes]),
		R = file:write(ChunkHdl, Bytes),
	    MM ! {send, R}, 
	    loop_replica(MM, ChunkID, MD5, ChunkHdl);
	{chan_closed, MM} ->
		{ok, FileName} 	= lib_common:get_file_name(ChunkID),
		{ok, LocalMD5}	= lib_md5:file(FileName),
		case LocalMD5 =:= MD5 of
			true ->
				data_db:add_chunkmeta_item(ChunkID, MD5),
				{ok, _HostName}= inet:gethostname();
				%%================= not implemented
				%gen_server:call(?META_SERVER, {registerchunk, ChunkID, list_to_atom(HostName)});
				%%==================
			false ->
				file:delete(FileName)
		end,	
		file:close(ChunkHdl),
		error_logger:info_msg("[~p, ~p]: dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.
