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
		{append, ChunkID, []}	->
			error_logger:info_msg("[~p, ~p]: append receive message:~p ~n", [?MODULE, ?LINE, {append, ChunkID, []}]),
			{ok, ChunkHdl} = lib_common:get_file_handle({append, ChunkID}),
		    loop_append(MM, ChunkID, ChunkHdl);
		{append, ChunkID, HostList}	->
			{ok, ChunkHdl} = lib_common:get_file_handle({append, ChunkID}),
			error_logger:info_msg("[~p, ~p]: append Message:~p ~n", [?MODULE, ?LINE, {append, ChunkID, HostList}]),
			%{ok, NextDataworkerPid} = lib_chan:connect(NextHost, ?DATA_PORT, dataworker,?PASSWORD,  {append, ChunkID, HostList}),
			{ok, NextDataworkerPid} = gen_server:call(data_server,{appendnext, ChunkID, HostList}),
			error_logger:info_msg("[~p, ~p]: append NextDataworkPid ~p ~n", [?MODULE, ?LINE, NextDataworkerPid]),
		    loop_append(MM, NextDataworkerPid, ChunkID, ChunkHdl);
	    {replica, ChunkID, MD5} ->
			{ok, ChunkHdl} = lib_common:get_file_handle({write, ChunkID}),
			loop_replica(MM, ChunkID, MD5, ChunkHdl);
	    {garbagecheck, []} ->
			loop_garbagecheck(MM, <<>> );			
	    {garbagecheck, HostList} ->
	    	[NextHost|LeftHosts]=HostList,
			{ok, NextDataworkerPid} = lib_chan:connect(NextHost, ?DATA_PORT, dataworker,?PASSWORD,  {garbagecheck, LeftHosts}),
			loop_garbagecheck(MM, NextDataworkerPid, <<>> )			
	end.

loop_append(MM, ChunkID, ChunkHdl) ->
    receive
	{chan, MM, {append, Bytes}} ->
		error_logger:info_msg("[~p, ~p]: append size ~p ~n", [?MODULE, ?LINE, size(Bytes)]),
		R = file:write(ChunkHdl, Bytes),
	    MM ! {send, R}, 
	    loop_append(MM, ChunkID, ChunkHdl);
	{chan_closed, MM} ->
		file:close(ChunkHdl),
		{ok, FileName} 	= lib_common:get_file_name(ChunkID),
		{ok, MD5}		= lib_md5:file(FileName),
		data_db:add_chunkmeta_item(ChunkID, MD5),		
		error_logger:info_msg("[~p, ~p]: append dataworker  ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.	

loop_append(MM, NextDataworkerPid, ChunkID, ChunkHdl) ->
    receive
	{chan, MM, {append, Bytes}} ->
		error_logger:info_msg("[~p, ~p]: append size ~p ~n", [?MODULE, ?LINE, size(Bytes)]),
		R = file:write(ChunkHdl, Bytes),
		R = rpc:call(NextDataworkerPid,{append, Bytes}),
	    MM ! {send, R}, 
	    loop_append(MM, NextDataworkerPid, ChunkID, ChunkHdl);
	{chan_closed, MM} ->
		file:close(ChunkHdl),
		gen_server:call(data_server,{disconnect, NextDataworkerPid}),
		{ok, FileName} 	= lib_common:get_file_name(ChunkID),
		{ok, MD5}		= lib_md5:file(FileName),
		data_db:add_chunkmeta_item(ChunkID, MD5),		
		error_logger:info_msg("[~p, ~p]: append dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.

loop_write(MM, ChunkID, ChunkHdl) ->
    receive
	{chan, MM, {write, Bytes}} ->
		error_logger:info_msg("[~p, ~p]: write size ~p ~n", [?MODULE, ?LINE, size(Bytes)]),
		R = file:write(ChunkHdl, Bytes),
	    MM ! {send, R}, 
	    loop_write(MM, ChunkID, ChunkHdl);
	{chan_closed, MM} ->
		file:close(ChunkHdl),
		{ok, FileName} 	= lib_common:get_file_name(ChunkID),
		{ok, MD5}		= lib_md5:file(FileName),
		data_db:add_chunkmeta_item(ChunkID, MD5),		
		error_logger:info_msg("[~p, ~p]: write dataworker  ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.

loop_read(MM, ChunkHdl) ->
    receive
	{chan, MM, {read, Start, Length}} ->
		R = file:pread(ChunkHdl, Start, Length),
		%error_logger:info_msg("[~p, ~p]: send size ~p ~n", [?MODULE, ?LINE, R]),		
	    MM ! {send, R},
	    loop_read(MM, ChunkHdl);
	{chan_closed, MM} ->
		file:close(ChunkHdl),
		%error_logger:info_msg("[~p, ~p]: read dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.

loop_replica(MM, ChunkID, MD5, ChunkHdl) ->
    receive
	{chan, MM, {replica, Bytes}} ->
		%error_logger:info_msg("[~p, ~p]: replica size ~p ~n", [?MODULE, ?LINE, size(Bytes)]),
		R = file:write(ChunkHdl, Bytes),
	    MM ! {send, R}, 
	    loop_replica(MM, ChunkID, MD5, ChunkHdl);
	{chan_closed, MM} ->
		file:close(ChunkHdl),
		{ok, FileName} 	= lib_common:get_file_name(ChunkID),
		{ok, LocalMD5}	= lib_md5:file(FileName),
		case LocalMD5 =:= MD5 of
			true ->
				data_db:add_chunkmeta_item(ChunkID, MD5),
				{ok, HostName}= inet:gethostname(),
				gen_server:call(?META_SERVER, {registerchunk, ChunkID, list_to_atom(HostName)});
			false ->
				file:delete(FileName)
		end,	
		%error_logger:info_msg("[~p, ~p]: replica dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.

loop_garbagecheck(MM, Bin) ->
    receive
	{chan, MM, {garbagecheck, BFBytes}} ->
		%error_logger:info_msg("[~p, ~p]: garbagecheck size ~p ~n", [?MODULE, ?LINE, size(BFBytes)]),
		Bin1 = list_to_binary(binary_to_list(Bin) ++ binary_to_list(BFBytes)),
	    loop_garbagecheck(MM, Bin1);
	{chan_closed, MM} ->
		BloomFilter = binary_to_term(Bin),
		ChunkList = data_db:select_chunklist_from_chunkmeta(),
		lists:foreach(fun(X) -> check_element(X, BloomFilter) end, ChunkList),		
		%error_logger:info_msg("[~p, ~p]: replica dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.
loop_garbagecheck(MM, NextDataworkPid, Bin) ->
    receive
	{chan, MM, {garbagecheck, BFBytes}} ->
		%error_logger:info_msg("[~p, ~p]: garbagecheck loop size ~p ~n", [?MODULE, ?LINE, size(BFBytes)]),
		lib_chan:rpc(NextDataworkPid,{garbagecheck, BFBytes}),
		Bin1 = list_to_binary(binary_to_list(Bin) ++ binary_to_list(BFBytes)),
	    loop_garbagecheck(MM, NextDataworkPid, Bin1);
	{chan_closed, MM} ->
		lib_chan:disconnect(NextDataworkPid),		
		BloomFilter = binary_to_term(Bin),
		ChunkList = data_db:select_chunklist_from_chunkmeta(),
		lists:foreach(fun(X) -> check_element(X, BloomFilter) end, ChunkList),		
		%error_logger:info_msg("[~p, ~p]: dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.    

check_element(ChunkID, BloomFilter) ->
	case bloom:is_element(ChunkID, BloomFilter) of
		true ->
			void;
		false ->
			{ok, FileName} 	= lib_common:get_file_name(ChunkID),
			file:delete(FileName),
			data_db:delete_chunkmeta_item(ChunkID)
	end.

