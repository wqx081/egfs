%%%-------------------------------------------------------------------
%%% File    : client_worker.erl
%%% Author  : Xiaomeng Huang
%%% Description : the client offer open/write/read/del functions
%%%
%%% Created :  9 Mar 2009 by Xiaomeng Huang 
%%%-------------------------------------------------------------------
-module(client_worker).
-behaviour(gen_server).
-include("../include/header.hrl").
-include_lib("kernel/include/file.hrl").
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%%====================================================================
%% gen_server callbacks
%%====================================================================
% do open operation
init([FileName, Mode, UserName]) ->
	process_flag(trap_exit,true),
	%error_logger:info_msg("[~p, ~p]: client worker ~p starting~n", [?MODULE, ?LINE, self()]),	
	case gen_server:call(?META_SERVER, {open, FileName, Mode, UserName}) of
	    {ok, FileID, FileSize, ChunkList, MetaWorkerPid} ->
			% set the correct position of FileContext 	
			NewFC = case Mode of
					  	read ->
							#filecontext{	fileid = FileID, 
											filename = FileName, 
				                 			filesize = FileSize,
											offset = 0,
											chunklist= ChunkList,
											mode = Mode,
											metaworkerpid = MetaWorkerPid};
						write ->
							#filecontext{	fileid = FileID, 
											filename = FileName, 
				                 			filesize = FileSize,
											offset = FileSize,
											chunklist= ChunkList,
											mode = Mode,
											metaworkerpid = MetaWorkerPid};
						append ->
							#filecontext{	fileid = FileID, 
											filename = FileName, 
				                 			filesize = FileSize,
											offset = FileSize,
											chunkid= ChunkList,
											mode = Mode,
											metaworkerpid = MetaWorkerPid}							
					  end,		
			% construct the FileContext based on the FileRecord and MetaWorkerPid
			link(MetaWorkerPid),
			{ok, NewFC};
		{error, Why} ->
			{stop, Why}
	end.	

handle_call({write, Bytes}, _From, FileContext) ->
	%error_logger:info_msg("[~p, ~p]: write ~p  ~n", [?MODULE, ?LINE, FileContext#filecontext.filename]),
	case FileContext#filecontext.mode of
		write ->
			{ok, NewFileContext} = write_data(FileContext, Bytes),	
			{reply, ok, NewFileContext};
		append ->
			{ok, NewFileContext} = append_data(FileContext, Bytes),	
			{reply, ok, NewFileContext};
		_Any ->
			{reply, {error, "write open mode error"}, FileContext}
	end;

handle_call({append, Bytes}, _From, FileContext) ->
	error_logger:info_msg("[~p, ~p]: append ~p  ~n", [?MODULE, ?LINE, FileContext#filecontext.filename]),
	case FileContext#filecontext.mode of
		append ->
			{ok, NewFileContext} = append_data(FileContext, Bytes),	
			{reply, ok, NewFileContext};
		_Any ->
			{reply, {error, "append open mode error"}, FileContext}
	end;

handle_call({read, Number}, _From, FileContext) ->
%	error_logger:info_msg("[~p, ~p]: read ~p  ~n", [?MODULE, ?LINE, FileContext#filecontext.filename]),	
	case do_read(FileContext, Number) of
		{ok, NewFC, Data} ->
			{reply, {ok, Data}, NewFC};	
		eof ->
			{reply, eof,FileContext};
		{error, Reason} ->
			{reply, {error,Reason}, FileContext}
		
	end;
	
handle_call({position, Location}, _From, FileContext) ->
	%error_logger:info_msg("[~p, ~p]: set position ~p  ~n", [?MODULE, ?LINE, Location]),	
	case do_position(FileContext, Location) of
		{ok, NewFC} ->
			{reply, ok, NewFC};
		{error, Reason} ->
			{reply, {error,Reason},FileContext}
	end;

handle_call({pread, Location, Number}, _From, FileContext) ->
	%error_logger:info_msg("[~p, ~p]: pread ~p  ~n", [?MODULE, ?LINE, FileContext#filecontext.filename]),	
	case do_position(FileContext, Location) of
		{ok, NewFC} ->
			case do_read(NewFC, Number) of
				{ok, NewFC1, Data} ->
					{reply, {ok,Data}, NewFC1};
				eof ->
					{reply, eof, FileContext};
				{error, Reason} ->
					{reply, {error, Reason}	, FileContext}
			end;
		{error, Reason} ->
			{reply, {error, Reason}	, FileContext}
	end;	
		
handle_call({close}, _From, FileContext) when FileContext#filecontext.mode=:=write ->
	%error_logger:info_msg("[~p, ~p]: close ~p ~n", [?MODULE, ?LINE, FileContext#filecontext.filename]),	
	#filecontext{	fileid	 = FileID, 
				 	filename = FileName, 
					filesize = FileSize, 
					chunkid	 = ChunkID,
					chunklist= PreChunkList,
					host	 = Host, 
					nodelist = PreNodeList,
					metaworkerpid= MetaWorkerPid,
					dataworkerpid= DataWorkerPid} = FileContext,
	ChunkList = case ChunkID=:=[] of
					true ->
						PreChunkList;
					false ->
					 	PreChunkList++[ChunkID]
				end, 
	NodeList = case Host=:=[] of
					true ->
						PreNodeList;
					false ->
					 	PreNodeList++[Host]
				end, 				
 	FileRecord = #filemeta{	id = FileID,
							name = FileName,
							size = FileSize,
							chunklist = ChunkList},	
	ChunkMappingRecords = generate_chunkmapping_record(ChunkList, NodeList),
	Reply = gen_server:call(MetaWorkerPid, {registerchunk, FileRecord, ChunkMappingRecords}),
	case DataWorkerPid =:= undefined of
		true ->
			void;
		false ->
			lib_chan:disconnect(DataWorkerPid)
	end,
	gen_server:cast(MetaWorkerPid, {stop,normal,self()}),	
	{stop, normal, Reply, #filecontext{}};
	
handle_call({close}, _From, FileContext) when FileContext#filecontext.mode=:=append ->
	error_logger:info_msg("[~p, ~p]: close ~p ~n", [?MODULE, ?LINE, FileContext#filecontext.filename]),	
	#filecontext{	fileid	 = FileID, 
				 	filename = FileName, 
					filesize = FileSize, 
					chunkid	 = ChunkID,
					chunklist= PreChunkList,
					host	 = Host, 
					nodelist = PreNodeList,
					metaworkerpid= MetaWorkerPid,
					dataworkerpid= DataWorkerPid} = FileContext,
	[ChunkList, NodeList] = case ChunkID of
								[] ->
									[PreChunkList, PreNodeList];
								_Any ->
									[PreChunkList ++ [ChunkID], PreNodeList ++ [Host]]
							end,	
	
 	FileRecord = #filemeta{	id = FileID,
							name = FileName,
							size = FileSize,
							chunklist = ChunkList},
	error_logger:info_msg("[~p, ~p]: FileRecord ChunkList ~p ~n", [?MODULE, ?LINE, FileRecord#filemeta.chunklist]),	
	case DataWorkerPid =:= undefined of
		true ->
			void;
		false ->
			lib_chan:disconnect(DataWorkerPid)
	end,
	ChunkMappingRecords = generate_chunkmapping_record(ChunkList, NodeList),
	Reply = gen_server:call(MetaWorkerPid, {registerchunk, FileRecord, ChunkMappingRecords}),
	gen_server:cast(MetaWorkerPid, {stop,normal,self()}),	
	{stop, normal, Reply, #filecontext{}};
	
handle_call({close}, _From, FileContext) when FileContext#filecontext.mode=:=read ->
	%error_logger:info_msg("[~p, ~p]: close ~p ~n", [?MODULE, ?LINE, FileContext#filecontext.filename]),	
	#filecontext{	metaworkerpid= MetaWorkerPid,
					dataworkerpid= DataWorkerPid} = FileContext,
	case DataWorkerPid =:= undefined of
		true ->
			void;
		false ->
			lib_chan:disconnect(DataWorkerPid)
	end,
	gen_server:cast(MetaWorkerPid, {stop,normal,self()}),		
	{stop, normal, ok, #filecontext{}}.

handle_cast(_Msg, FileContext) ->
    {noreply, FileContext}.

handle_info({'EXIT', _Pid, Why}, FileContext) ->
	%error_logger:info_msg("[~p, ~p]: receive EXIT message from ~p since ~p~n", [?MODULE, ?LINE, Pid,Why]),	
	{stop, Why, FileContext};	    
handle_info(_Info, FileContext) ->
    {noreply, FileContext}.

terminate(_Reason, _FileContext) ->
	%error_logger:info_msg("[~p, ~p]: close client worker ~p  since ~p~n", [?MODULE, ?LINE, self(),Reason]),	 
    ok.

code_change(_OldVsn, FileContext, _Extra) ->
    {ok, FileContext}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
% if the data is null, write finish.
write_data(FileContext, Bytes) when size(Bytes) =:= 0 ->	
	%error_logger:info_msg("[~p, ~p]:A write ~p~n", [?MODULE, ?LINE, size(Bytes)]),
	{ok, FileContext};
% if the data is wrote into a new chunkFileContext#filecontext
write_data(FileContext, Bytes) when FileContext#filecontext.dataworkerpid =:= undefined  ->
	%error_logger:info_msg("[~p, ~p]:B write ~p~n", [?MODULE, ?LINE, size(Bytes)]),	
	ChunkID=lib_uuid:gen(),
	{ok,SelectedHost} = gen_server:call(?HOST_SERVER, {allocate_dataserver}),
	{ok, DataWorkPid} = lib_chan:connect(SelectedHost, ?DATA_PORT, dataworker,?PASSWORD,  {write, ChunkID}),
	NewFC= FileContext#filecontext{	dataworkerpid  = DataWorkPid, 
									chunkid = ChunkID,
									host    = SelectedHost},
	write_data(NewFC, Bytes);
% if the data is wrote into a existed chunk
write_data(FileContext, Bytes) ->
	#filecontext{	offset = Offset,
					chunklist=ChunkList,
					nodelist= NodeList,
					chunkid=ChunkID,
					host=Host,
				 	dataworkerpid = DataWorkerPid} = FileContext,
	Start		= Offset rem ?CHUNKSIZE,
	WantLength	= ?CHUNKSIZE-Start,
	Number		= size(Bytes),
	ReadLength	= lists:min([Number, WantLength]),
	case Start+ReadLength =:= ?CHUNKSIZE of
		true ->		 
			%error_logger:info_msg("[~p, ~p]:C write ~p~n", [?MODULE, ?LINE, size(Bytes)]),
			{Right, Left} = split_binary(Bytes, ReadLength),
			lib_chan:rpc(DataWorkerPid,{write,Right}),
			% close the data worker and reset the FileContext
			lib_chan:disconnect(DataWorkerPid),		
			NewChunkList= ChunkList ++ [ChunkID],			
			NewNodeList= NodeList ++ [Host],				
			NewFC= FileContext#filecontext{	offset = Offset+ReadLength, 
											filesize= Offset+ReadLength,
											dataworkerpid=undefined,
											chunkid=[],
											host=[],
											chunklist=NewChunkList,
											nodelist=NewNodeList},
			% write the left data
			write_data(NewFC, Left);
		false ->
			%error_logger:info_msg("[~p, ~p]:D write ~p~n", [?MODULE, ?LINE, Number]),
			lib_chan:rpc(DataWorkerPid,{write,Bytes}),
			NewFC= FileContext#filecontext{	offset = Offset + ReadLength,
											filesize=Offset + ReadLength},
			{ok, NewFC}			
	end.

do_position(FileContext, Location) ->
	#filecontext{	offset   = Offset,
					filesize = FileSize,
				 	dataworkerpid = DataWorkerPid} = FileContext,
	case Location =<  FileSize of
	    true ->
			TargetChunkIndex= Location div ?CHUNKSIZE,
			CurrentChunkIndex= Offset div ?CHUNKSIZE,
			case TargetChunkIndex =:= CurrentChunkIndex of
				true ->
					NewFC= FileContext#filecontext{	offset = Location},
			    	{ok, NewFC};
				false ->
					case DataWorkerPid=:=undefined of 
						true ->
							void;
						false ->
							lib_chan:disconnect(DataWorkerPid)
					end,
					NewFC= FileContext#filecontext{	offset = Location, 													
													chunkid=[],
													host=[],
													dataworkerpid=undefined},
					{ok, NewFC}
			end;
    	false ->
			{error, "set wrong location"}
	end.

do_read(FileContext, Number) ->
	case FileContext#filecontext.mode of
		read ->
			case FileContext#filecontext.offset =:= FileContext#filecontext.filesize of
			    true ->
			    	eof;
		    	false ->
		    		read_data(FileContext, Number)
			end;			
		_Any ->
			{error, "read open mode error"}
	end.
	
read_data(FileContext, Number) ->
	read_data(FileContext, Number, []).
read_data(FileContext, 0, L) ->
	%error_logger:info_msg("[~p, ~p]:A read ~p~n", [?MODULE, ?LINE, L]),
	{ok, FileContext, list_to_binary(L)};
read_data(FileContext, _Number, L) when FileContext#filecontext.offset =:= FileContext#filecontext.filesize ->	
	%error_logger:info_msg("[~p, ~p]:B read ~p~n", [?MODULE, ?LINE, L]),
	{ok, FileContext, list_to_binary(L)};
read_data(FileContext, Number, L) when FileContext#filecontext.dataworkerpid =:= undefined  ->	
	%error_logger:info_msg("[~p, ~p]:C read ~p~n", [?MODULE, ?LINE, L]),
	#filecontext{offset=Offset,
				 chunklist = ChunkList,			 
				 metaworkerpid = MetaWorkerPid} = FileContext,
	Index 	= Offset div ?CHUNKSIZE,
	ChunkID = lists:nth(Index+1, ChunkList),
	Hosts = gen_server:call(MetaWorkerPid, {seekchunk, ChunkID}),
	[Host|_T] = Hosts,
	{ok, DataWorkPid} = lib_chan:connect(Host, ?DATA_PORT, dataworker,?PASSWORD, {read, ChunkID}),
	NewFC= FileContext#filecontext{	dataworkerpid  = DataWorkPid, 
									chunkid = ChunkID,
									host    = Host},
	read_data(NewFC, Number, L);
read_data(FileContext, Number, L) ->
	#filecontext{filesize=FileSize,
				 offset=Offset,
				 dataworkerpid = DataWorkerPid} = FileContext,
	Start		= Offset rem ?CHUNKSIZE,
	WantLength	= ?CHUNKSIZE-Start,
	ReadLength	= lists:min([Number, WantLength, FileSize-Offset]),
	{ok, Data} = lib_chan:rpc(DataWorkerPid,{read,Start,ReadLength}), 
	L1= L ++ binary_to_list(Data),
	case Start+ReadLength =:= ?CHUNKSIZE of
		true ->
			%error_logger:info_msg("[~p, ~p]:D read ~p~n", [?MODULE, ?LINE, Data]),
			lib_chan:disconnect(DataWorkerPid),	
			NewFC= FileContext#filecontext{	dataworkerpid=undefined,
											offset = Offset+ReadLength},
								
			read_data(NewFC, Number-ReadLength, L1);	
		false ->
			%error_logger:info_msg("[~p, ~p]:E read ~p~n", [?MODULE, ?LINE, Data]),
			NewFC= FileContext#filecontext{	offset = Offset+ReadLength},			
			read_data(NewFC, Number-ReadLength, L1)
	end.	
	
	
% if the data is null, append finish.
append_data(FileContext, Bytes) when size(Bytes) =:= 0 ->	
	%error_logger:info_msg("[~p, ~p]:A append ~p~n", [?MODULE, ?LINE, size(Bytes)]),
	{ok, FileContext};
% if the data is appended into a new chunkFileContext#filecontext
append_data(FileContext, Bytes) when FileContext#filecontext.dataworkerpid =:= undefined ->
	#filecontext{	offset = Offset,
					chunkid=ChunkID,
					metaworkerpid = MetaWorkerPid} = FileContext,	
	NewFC= 	case (Offset rem ?CHUNKSIZE=:=0) of
				true ->
					%error_logger:info_msg("[~p, ~p]:B append ~p~n", [?MODULE, ?LINE, size(Bytes)]),
					NewChunkID=lib_uuid:gen(),
					{ok,SelectedHost} = gen_server:call(?HOST_SERVER, {allocate_dataserver}),
					{ok, DataWorkPid} = lib_chan:connect(SelectedHost, ?DATA_PORT, dataworker,?PASSWORD,  {append, NewChunkID, []}),
					FileContext#filecontext{dataworkerpid  = DataWorkPid, 
											chunkid	= NewChunkID,
											host    = SelectedHost};
				false ->
					%error_logger:info_msg("[~p, ~p]:C append ~p~n", [?MODULE, ?LINE, size(Bytes)]),
					[SelectedHost|LeftHosts] = gen_server:call(MetaWorkerPid, {seekchunk, ChunkID}),	
					{ok, DataWorkerPid} = lib_chan:connect(SelectedHost, ?DATA_PORT, dataworker,?PASSWORD,  {append, ChunkID, LeftHosts}),
					FileContext#filecontext{dataworkerpid  = DataWorkerPid,
											chunkid=[],
											host = []}
			end,
	append_data(NewFC, Bytes);	
% if the data is appended into a existed chunk
append_data(FileContext, Bytes) ->
	#filecontext{	offset = Offset,
					chunklist=ChunkList,
					nodelist= NodeList,
					chunkid=ChunkID,
					host=Host,
				 	dataworkerpid = DataWorkerPid} = FileContext,
	Start		= Offset rem ?CHUNKSIZE,
	WantLength	= ?CHUNKSIZE-Start,
	Number		= size(Bytes),
	ReadLength	= lists:min([Number, WantLength]),
	case Start+ReadLength =:= ?CHUNKSIZE of
		true ->	 
			%error_logger:info_msg("[~p, ~p]:D append ~p~n", [?MODULE, ?LINE, size(Bytes)]),
			{Right, Left} = split_binary(Bytes, ReadLength),
			lib_chan:rpc(DataWorkerPid,{append,Right}),
			% close the data worker and reset the FileContext
			lib_chan:disconnect(DataWorkerPid),	
			[NewChunkList, NewNodeList] = 	case ChunkID of
												[] ->
													[ChunkList, NodeList];
												_Any ->
													[ChunkList ++ [ChunkID], NodeList ++ [Host]]
											end,	
			NewFC= FileContext#filecontext{	offset = Offset+ReadLength, 
											filesize= Offset+ReadLength,
											dataworkerpid=undefined,
											chunkid=[],
											host=[],
											chunklist=NewChunkList,
											nodelist=NewNodeList},
			% write the left data
			append_data(NewFC, Left);
		false ->
			%error_logger:info_msg("[~p, ~p]:E append ~p~n", [?MODULE, ?LINE, Number]),
			lib_chan:rpc(DataWorkerPid,{append,Bytes}),
			NewFC= FileContext#filecontext{	offset = Offset + ReadLength,
											filesize=Offset + ReadLength},
			{ok, NewFC}			
	end.	
	
generate_chunkmapping_record([],[]) ->
	[];
generate_chunkmapping_record([CH|CT],[NH|NT]) ->
	[#chunkmapping{chunkid=CH, chunklocations=[NH]}] ++ generate_chunkmapping_record(CT,NT).	
