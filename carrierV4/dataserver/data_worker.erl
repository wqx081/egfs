-module(data_worker).
-include("../include/header.hrl").
-export([run/3]).

run(MM, ArgC, _ArgS) ->
 	error_logger:info_msg("[~p, ~p]: dataworker ~p running ArgC =~p ~n", [?MODULE, ?LINE, self(),ArgC]),
	% Argc =:= {write, ChunkID} | {read, ChunkID}
	{ok, ChunkHdl} = lib_common:get_file_handle(ArgC),
    loop(MM, ChunkHdl).

loop(MM, ChunkHdl) ->
    receive
	{chan, MM, {write, Bytes}} ->
		%error_logger:info_msg("[~p, ~p]: write ~p ~n", [?MODULE, ?LINE, Bytes]),
		R = file:write(ChunkHdl, Bytes),
	    MM ! {send, R}, 
	    loop(MM, ChunkHdl);
	{chan, MM, {read, Start, Length}} ->
		R = file:pread(ChunkHdl, Start, Length),
		%error_logger:info_msg("[~p, ~p]: send ~p ~n", [?MODULE, ?LINE, R]),		
	    MM ! {send, R},
	    loop(MM, ChunkHdl);
	{chan_closed, MM} ->
		file:close(ChunkHdl),
		%error_logger:info_msg("[~p, ~p]: dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.

   
    

