-module(data_server).
-include("../include/header.hrl").
-export([start_link/0, loop_replica/2]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
%	timer:apply_interval(3000, ?MODULE, heartbeat, []),
%	timer:apply_interval(86400000, ?MODULE, md5check, []),
    data_db:start(),	
	timer:apply_interval(?MD5CHECK_TIMER, data_md5check, md5check, []),
	timer:apply_after(1, data_bootreport, bootreport, []),	
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
	process_flag(trap_exit,true),
	{ok, HostName}= inet:gethostname(),
	case gen_server:call(?HOST_SERVER, {register_dataserver, list_to_atom(HostName), undefined, undefined, uplink}) of
		ok 	 ->
		    error_logger:info_msg("[~p, ~p]: dataserver ~p starting~n", [?MODULE, ?LINE, list_to_atom(HostName)]),
		   	lib_chan:start_server("./data_config");		    
		_Any ->
			error_logger:info_msg("[~p, ~p]: dataserver ~p init failed~n", [?MODULE, ?LINE, list_to_atom(HostName)])
	end,
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast({replica, string(), binary()}, State) -> {noreply, State} 
%% Description: This message is casted from Meta Server. 
%%				Ask the local data server writes a new replica to DestHost.
%%--------------------------------------------------------------------
handle_cast({replica, DestHost, ChunkID}, State) ->
	case data_db:select_md5_from_chunkmeta_id(ChunkID) of
		[MD5] ->
			{ok, DataWorkPid} = lib_chan:connect(DestHost, 8888, dataworker,?PASSWORD, {replica, ChunkID,MD5}),
			{ok, ChunkHdl}=	lib_common:get_file_handle({read, ChunkID}),
			loop_replica(DataWorkPid,ChunkHdl),
			{noreply, State};
		[] ->
			{noreply, State}
	end;
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(Reason, _State) ->
	error_logger:info_msg("[~p, ~p]: close dataserver ~p since ~p~n", [?MODULE, ?LINE, self(),Reason]),	 
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
loop_replica(DataWorkerPid, ChunkHdl) ->
	case file:read(ChunkHdl,?STRIP_SIZE) of % read 128K every time 
		{ok, Data} ->
			lib_chan:rpc(DataWorkerPid,{replica,Data}), 	
			loop_replica(DataWorkerPid, ChunkHdl);
		eof ->
			lib_chan:disconnect(DataWorkerPid),
			file:close(ChunkHdl);	
		{error,Reason} ->
			error_logger:error_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE, Reason]),	 
			lib_chan:disconnect(DataWorkerPid),
			file:close(ChunkHdl)
	end.	
	
	
