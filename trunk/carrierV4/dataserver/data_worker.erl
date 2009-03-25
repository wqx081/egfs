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
					loop_append(MM, undefined, ChunkID, ChunkHdl);
				[NextHost|LeftHosts] ->
					{ok, NextDataworkerPid} = gen_server:call(data_server,{connectnext, NextHost, {append, ChunkID, LeftHosts}}),
					loop_append(MM, NextDataworkerPid, ChunkID, ChunkHdl)	
			end;
	    {replica, ChunkID, MD5} ->
			{ok, ChunkHdl} = lib_common:get_file_handle({write, ChunkID}),
			loop_replica(MM, ChunkID, MD5, ChunkHdl);
	    {garbagecheck, HostList} ->
			case HostList of
				[] ->
					loop_garbagecheck(MM, undefined,  <<>>);
				[NextHost|LeftHosts] ->
					{ok, NextDataworkerPid} = gen_server:call(data_server,{connectnext, NextHost, {garbagecheck, LeftHosts}}),
					loop_garbagecheck(MM, NextDataworkerPid, <<>> )
			end				
	end.

loop_append(MM, NextDataworkerPid, ChunkID, ChunkHdl) ->
    receive
	{chan, MM, {append, Bytes}} ->
		%error_logger:info_msg("[~p, ~p]: append ~p NextDataworkerPid ~p~n", [?MODULE, ?LINE, Bytes,NextDataworkerPid]),
		R=file:write(ChunkHdl, Bytes),
		case NextDataworkerPid of
			undefined ->
				%error_logger:info_msg("[~p, ~p]: No next dataworker to append~n", [?MODULE, ?LINE]),					
				MM ! {send, R};
			_Any ->
				case gen_server:call(data_server,{rpcnext, NextDataworkerPid, {append, Bytes}}) of
					ok ->
						%error_logger:info_msg("[~p, ~p]: append next dataworker successfully~n", [?MODULE, ?LINE]),
						MM ! {send, R};
					_Error ->
						MM ! {send, error}		
				end   	
		end,		
	    loop_append(MM, NextDataworkerPid, ChunkID, ChunkHdl);
	{chan_closed, MM} ->
		file:close(ChunkHdl),
		case NextDataworkerPid of
			undefined ->
				%error_logger:info_msg("[~p, ~p]: No next dataworker to close~n", [?MODULE, ?LINE]),					
				void;
			_Any ->
				gen_server:cast(data_server,{closenext, NextDataworkerPid})
				%error_logger:info_msg("[~p, ~p]: close next dataworker successfully~n", [?MODULE, ?LINE])
		end,  
		{ok, FileName} 	= lib_common:get_file_name(ChunkID),
		{ok, MD5}		= lib_md5:file(FileName),
		{ok, ChunkSize} = lib_common:get_file_size(FileName),
		data_db:add_chunkmeta_item(ChunkID, MD5, ChunkSize),		
		error_logger:info_msg("[~p, ~p]: append dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
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
		{ok, ChunkSize} = lib_common:get_file_size(FileName),
		data_db:add_chunkmeta_item(ChunkID, MD5, ChunkSize),		
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
		error_logger:info_msg("[~p, ~p]: read dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
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
				{ok, ChunkSize} = lib_common:get_file_size(FileName),
				data_db:add_chunkmeta_item(ChunkID, MD5, ChunkSize),
				{ok, HostName}= inet:gethostname(),
				gen_server:call(?META_SERVER, {registerchunk, ChunkID, list_to_atom(HostName)});
			false ->
				file:delete(FileName)
		end,	
		error_logger:info_msg("[~p, ~p]: replica dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.

loop_garbagecheck(MM, NextDataworkerPid, Bin) ->
    receive
	{chan, MM, {garbagecheck, BFBytes}} ->
		error_logger:info_msg("[~p, ~p]: garbagecheck loop size ~p ~n", [?MODULE, ?LINE, size(BFBytes)]),
		case NextDataworkerPid of
			undefined ->
				error_logger:info_msg("[~p, ~p]: No next dataworker to close~n", [?MODULE, ?LINE]),					
				MM ! {send, ok};
			_Any ->
				case gen_server:call(data_server,{rpcnext, NextDataworkerPid, {garbagecheck, BFBytes}}) of
					ok ->
						error_logger:info_msg("[~p, ~p]: garbagecheck next dataworker successfully~n", [?MODULE, ?LINE]),
						MM ! {send, ok};
					_Error ->
						MM ! {send, error}		
				end
		end, 
		Bin1 = list_to_binary(binary_to_list(Bin) ++ binary_to_list(BFBytes)),
		%error_logger:info_msg("[~p, ~p]: Bin1:~p~n", [?MODULE, ?LINE, Bin1]),		
	    loop_garbagecheck(MM, NextDataworkerPid, Bin1);
	{chan_closed, MM} ->
		case NextDataworkerPid of
			undefined ->
				void;
			_Any ->
				gen_server:cast(data_server,{closenext, NextDataworkerPid})
		end,
		BloomFilter = binary_to_term(Bin),
		ChunkList = data_db:select_chunklist_from_chunkmeta(),
		lists:foreach(fun(X) -> check_element(X, BloomFilter) end, ChunkList),		
		error_logger:info_msg("[~p, ~p]: garbagecheck dataworker ~p stopping~n", [?MODULE, ?LINE,self()]),
	    exit(normal)
    end.    

check_element(ChunkID, BloomFilter) ->
	case lib_bloom:is_element(ChunkID, BloomFilter) of
		true ->
			error_logger:info_msg("[~p, ~p]: A:ChunID ~p is element~n", [?MODULE, ?LINE,ChunkID]),
			void;
		false ->
			error_logger:info_msg("[~p, ~p]: B:ChunID ~p is not element~n", [?MODULE, ?LINE,ChunkID]),
			{ok, FileName} 	= lib_common:get_file_name(ChunkID),
			file:delete(FileName),
			data_db:delete_chunkmeta_item(ChunkID)
	end.
