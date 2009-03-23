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
		{append, ChunkID, HostList}	->
			{ok, ChunkHdl} = lib_common:get_file_handle({append, ChunkID}),
			case HostList of
				[] ->
					loop_append(MM, [], ChunkID, ChunkHdl);
				[NextHost|LeftHosts] ->
					{ok, NextDataworkerPid} = do_nextdataworker(fun() -> lib_chan:connect(NextHost, ?DATA_PORT, dataworker, ?PASSWORD, {append, ChunkID, LeftHosts}) end),
					error_logger:info_msg("[~p, ~p]: NextDataworkerPid ~p ~n", [?MODULE, ?LINE, NextDataworkerPid]),
					loop_append(MM, NextDataworkerPid, ChunkID, ChunkHdl)
			end;
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

loop_write(MM, ChunkID, ChunkHdl) ->
    receive
	{chan, MM, {write, Bytes}} ->
		%error_logger:info_msg("[~p, ~p]: write size ~p ~n", [?MODULE, ?LINE, size(Bytes)]),
		R = file:write(ChunkHdl, Bytes),
	    MM ! {send, R}, 
	    loop_write(MM, ChunkID, ChunkHdl);
	{chan_closed, MM} ->
		file:close(ChunkHdl),
		{ok, FileName} 	= lib_common:get_file_name(ChunkID),
		{ok, MD5}		= lib_md5:file(FileName),
		data_db:add_chunkmeta_item(ChunkID, MD5),		
		%error_logger:info_msg("[~p, ~p]: write dataworker  ~p stopping~n", [?MODULE, ?LINE,self()]),
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

loop_append(MM, NextDataworkerPid, ChunkID, ChunkHdl) ->
    receive
	{chan, MM, {append, Bytes}} ->
		error_logger:info_msg("[~p, ~p]: append size ~p ~n", [?MODULE, ?LINE, size(Bytes)]),
		R =file:write(ChunkHdl, Bytes),
   		MM ! {send, R}, 		
		case NextDataworkerPid of
			[] ->
				void;
			_Any ->
				RR = do_nextdataworker(fun() -> lib_chan:rpc(NextDataworkerPid, {append, Bytes}) end),
				error_logger:info_msg("[~p, ~p]: RR ~p ~n", [?MODULE, ?LINE, RR])		
				%Pid = spawn_link(fun() -> NextDataworkerPid!{send, {append, Bytes}} end),
		end,	

	    loop_append(MM, NextDataworkerPid, ChunkID, ChunkHdl);
	{chan_closed, MM} ->
		file:close(ChunkHdl),
		case NextDataworkerPid of
			[] ->
				void;
			_Any ->
				do_nextdataworker(fun() -> lib_chan:rpc(NextDataworkerPid, {close}) end)
		end,
		{ok, FileName} 	= lib_common:get_file_name(ChunkID),
		{ok, MD5}		= lib_md5:file(FileName),
		data_db:add_chunkmeta_item(ChunkID, MD5),		
		error_logger:info_msg("[~p, ~p]: append dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
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



do_nextdataworker(F) ->
	error_logger:info_msg("[~p, ~p]: do nextdataworker~n", [?MODULE, ?LINE]),
	Parent=self(),
	Pid = spawn_link(fun() ->
						Reply=F(),
						error_logger:info_msg("[~p, ~p]:Subprocess Reply:~p~n", [?MODULE, ?LINE, Reply]),	
						Parent!{reply, self(), Reply}
					end),
	receive
		{reply, Pid, Reply} ->
			Reply
	after 10000->
		error_logger:info_msg("[~p, ~p]:Timeout:AAABBBCCC~n", [?MODULE, ?LINE]),	
		true
	end.

