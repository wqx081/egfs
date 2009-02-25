-module(data_worker).
-include("../include/header.hrl").
-export([run/3]).

run(MM, ArgC, _ArgS) ->
 	error_logger:info_msg("[~p, ~p]: dataworker ~p running ArgC =~p ~n", [?MODULE, ?LINE, self(),ArgC]),
	% Argc =:= {write, ChunkID} | {read, ChunkID}
	{_, ChunkID}=ArgC,
	{ok, ChunkHdl} = lib_common:get_file_handle(ArgC),
    loop(MM, ChunkID, ChunkHdl).

loop(MM, ChunkID, ChunkHdl) ->
    receive
	{chan, MM, {write, Bytes}} ->
		%error_logger:info_msg("[~p, ~p]: write ~p ~n", [?MODULE, ?LINE, Bytes]),
		R = file:write(ChunkHdl, Bytes),
	    MM ! {send, R}, 
	    loop(MM, ChunkID, ChunkHdl);
	{chan, MM, {read, Start, Length}} ->
		R = file:pread(ChunkHdl, Start, Length),
		%error_logger:info_msg("[~p, ~p]: send ~p ~n", [?MODULE, ?LINE, R]),		
	    MM ! {send, R},
	    loop(MM, ChunkID, ChunkHdl);
	{chan_closed, MM} ->
		{ok, FileName} 	= lib_common:get_file_name(ChunkID),
		{ok, MD5}		= lib_md5:file(FileName),
		data_db:add_chunkmeta_item(ChunkID, MD5),		
		file:close(ChunkHdl),
		error_logger:info_msg("[~p, ~p]: dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.

   
    

