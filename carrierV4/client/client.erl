%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : Xiaomeng Huang
%%% Description : the client offer open/write/read/del functions
%%%
%%% Created :  30 Jan 2009 by Xiaomeng Huang 
%%%-------------------------------------------------------------------
-module(client).
-behaviour(gen_server).
-include("../include/header.hrl").
-include_lib("kernel/include/file.hrl").
-export([	start_link/0, 
			open/2, 
			write/2, 
			read/2, 
			close/1, 
			delete/1, 
			testw/1,
			testr/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).


%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error, Reason}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    error_logger:info_msg("[~p, ~p]: client ~p starting ~n", [?MODULE, ?LINE, node()]),
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------------------
%% Function: open(string(),Mode) -> {ok, FileContext} | {error, Reason} 
%% 			 Mode = r | w 
%% Description: open a file according to the filename and open mode
%%--------------------------------------------------------------------------------
open(FileName, Mode) ->
	gen_server:call(?MODULE, {open, FileName, Mode}).

%% --------------------------------------------------------------------
%% Function: write(binary()) ->  {ok, FileContext} | {error, Reason} 
%% Description: write Bytes to dataserver
%% --------------------------------------------------------------------
write(FileContext, Bytes) ->
	gen_server:call(?MODULE, {write, FileContext, Bytes},120000).

%% --------------------------------------------------------------------
%% Function: read(int()) ->  {ok, FileContext, Data} | {eof, FileContext} | {error, Reason} 
%% Description: read Number Bytes from Location offset
%% --------------------------------------------------------------------
read(FileContext, Number) ->
	gen_server:call(?MODULE, {read, FileContext, Number}, 120000).
	
%% --------------------------------------------------------------------
%% Function: close/1 ->  ok | {error, Reason} 
%% Description: close file
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------
close(FileContext) ->
	gen_server:call(?MODULE, {close,FileContext}).
	
%% --------------------------------------------------------------------
%% Function: delete/1 ->  ok | {error, Reason} 
%% Description: delete file
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------
delete(FileName) ->
	gen_server:call(?MODULE, {delete, FileName}).	
	
%%====================================================================
%% gen_server callbacks
%%====================================================================
init([]) ->
	%process_flag(trap_exit,true),
    {ok, #filecontext{}}.

handle_call({open, FileName, Mode}, _From, State) ->
	error_logger:info_msg("[~p, ~p]: open file ~p mode ~p~n", [?MODULE, ?LINE, FileName, Mode]),	
	case gen_server:call(?META_SERVER, {open, FileName, Mode}) of
	    {ok, FileID, FileSize, ChunkList, MetaWorkerPid} ->
			% set the correct position of FileContext 	
			Offset = case Mode of 
					  	r ->
							0;
						w ->
							FileSize;
						a ->
							FileSize									
					  end,		
			% construct the FileContext based on the FileRecord and MetaWorkerPid
			FileContext = #filecontext{	fileid = FileID, 
										filename = FileName, 
				                 		filesize = FileSize,
										offset = Offset,
										chunklist= ChunkList,
										mode = Mode,
										metaworkerpid = MetaWorkerPid},
			link(MetaWorkerPid),
			{reply, {ok, FileContext}, State};
		{error, Why} ->
			{reply, {error, Why}, State}
	end;

handle_call({write, FileContext, Bytes}, _From, State) ->
%	error_logger:info_msg("[~p, ~p]: write ~p  ~n", [?MODULE, ?LINE, FileContext#filecontext.filename]),
	case FileContext#filecontext.mode of
		w	->
			{ok, NewFileContext} = write_data(FileContext, Bytes),	
			{reply, {ok, NewFileContext}, State};
		_Any ->
			{reply, {error, "write open mode error"}, State}
	end;
	
handle_call({read, FileContext, Number}, _From, State) ->
%	error_logger:info_msg("[~p, ~p]: read ~p  ~n", [?MODULE, ?LINE, FileContext#filecontext.filename]),	
	case FileContext#filecontext.mode of
		r	->
			case FileContext#filecontext.offset =:= FileContext#filecontext.filesize of
			    true ->
			    	{reply, {eof, FileContext} , State};
		    	false ->
		    		{ok, NewFileContext, Data} = read_data(FileContext, Number),	
					{reply, {ok, NewFileContext, Data}, State}
			end;			
		_Any ->
			{reply, {error, "read open mode error"}, State}
	end;
		
handle_call({close, FileContext}, _From, State) when FileContext#filecontext.mode=:=w ->
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

 	FileRecord = #filemeta{	fileid = FileID,
							filename = FileName,
							filesize = FileSize,
							chunklist = ChunkList},	
	ChunkMappingRecords = generate_chunkmapping_record(ChunkList, NodeList),
	Reply = gen_server:call(MetaWorkerPid, {registerchunk, FileRecord, ChunkMappingRecords}),
	case DataWorkerPid =:= undefined of
		true ->
			void;
		false ->
			lib_chan:disconnect(DataWorkerPid)
	end,
	gen_server:cast(MetaWorkerPid, {stop,normal}),	
	{reply, Reply, State};
handle_call({close, FileContext}, _From, State) when FileContext#filecontext.mode=:=r ->
	error_logger:info_msg("[~p, ~p]: close ~p ~n", [?MODULE, ?LINE, FileContext#filecontext.filename]),	
	#filecontext{	metaworkerpid= MetaWorkerPid,
					dataworkerpid= DataWorkerPid} = FileContext,
	case DataWorkerPid =:= undefined of
		true ->
			void;
		false ->
			lib_chan:disconnect(DataWorkerPid)
	end,
	gen_server:cast(MetaWorkerPid, {stop,normal}),		
	{reply, ok, State};	

handle_call({delete, FileName}, _From, State)  ->
	error_logger:info_msg("[~p, ~p]: delete ~p ~n", [?MODULE, ?LINE, FileName]),	
	Reply = gen_server:cast(?META_SERVER, {delete, FileName}),					
	{reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Why}, State) ->
	error_logger:info_msg("[~p, ~p]: receive EXIT message from ~p since ~p~n", [?MODULE, ?LINE, Pid,Why]),	
	{stop, Why, State};	    
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State) ->
	error_logger:info_msg("[~p, ~p]: close client ~p  since ~p~n", [?MODULE, ?LINE, self(),Reason]),	 
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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
			NewFC= FileContext#filecontext{	offset = Offset+ReadLength,
											filesize=Offset+ReadLength},
			{ok, NewFC}			
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
	[Hosts] = gen_server:call(MetaWorkerPid, {seekchunk, ChunkID}),
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
			
generate_chunkmapping_record([],[]) ->
	[];
generate_chunkmapping_record([CH|CT],[NH|NT]) ->
	[#chunkmapping{chunkid=CH, chunklocations=[NH]}] ++ generate_chunkmapping_record(CT,NT).
	
% --------------------------------------------------------------------
%% Function: pread/3
%% Description: read Bytes from dataserver
%% Returns: {ok, Binary} | {error, Reason}          |
%% --------------------------------------------------------------------
%pread(FileDevice, Start, Length) ->
%    do_pread(FileDevice, Start, Length).

%% --------------------------------------------------------------------
%% Function: close/1
%% Description: delete file which at dataserver
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------
%close(FileDevice) ->
%    do_close(FileDevice).

%% --------------------------------------------------------------------
%% test function
%% --------------------------------------------------------------------

testw(FileName) ->
	case open(FileName,w) of
		{ok, FileContext}  ->
			{ok,Hdl}=file:open("a.rmvb",[binary,raw,read,read_ahead]),
			NewFileContext=write_loop(FileContext, Hdl),
			close(NewFileContext);
		{error, Why} ->
			Why
	end.
	
write_loop(FileContext, Hdl)->
	case file:read(Hdl,?STRIP_SIZE) of % read 128K every time 
		{ok, Data} ->
			{ok, NewFileContext} = write(FileContext, Data),
			write_loop(NewFileContext, Hdl);
		eof ->
			file:close(Hdl),
			FileContext;	
		{error,Reason} ->
			Reason
	end.	

testr(FileName) ->
	case open(FileName,r) of
		{ok, FileContext}   ->
			{ok,Hdl}=file:open("b.rmvb",[binary,raw,write]),
			NewFileContext=read_loop(FileContext,Hdl),
			close(NewFileContext);
		{error, Why} ->
			Why
	end.
	
read_loop(FileContext, Hdl)->
	case read(FileContext,?STRIP_SIZE) of % read 128K every time 
		{ok, NewFileContext, Data} ->
			file:write(Hdl,Data),
			read_loop(NewFileContext, Hdl);
		{eof,NewFileContext} ->
			file:close(Hdl),
			NewFileContext;
		{error,Reason} ->
			Reason
	end.
	
