%%%-------------------------------------------------------------------
%%% File    : client_server.erl
%%% Author  : Xiaomeng Huang
%%% Description : the client offer open/del/mkdir/.... functions
%%%
%%% Created :  9 Mar 2009 by Xiaomeng Huang 
%%%-------------------------------------------------------------------
-module(client_server).
-behaviour(gen_server).
-include("../include/header.hrl").
-include_lib("kernel/include/file.hrl").
-export([	start_link/0, 
			open/3, 
			close/1, 
			delete/2,
			copy/3,
			move/3,
			listdir/2,
			mkdir/2,
			getfileinfo/2,
			chmod/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).


%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error, Reason}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    error_logger:info_msg("[~p, ~p]: client server ~p starting ~n", [?MODULE, ?LINE, node()]),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------------------
%% Function: open(string(),Mode) -> {ok, ClientWorkerPid} | {error, Reason} 
%% 			 Mode = r | w 
%% Description: open a file according to the filename and open mode
%%--------------------------------------------------------------------------------
open(FileName, Mode, UserName) ->
	gen_server:call(?MODULE, {open, FileName, Mode, UserName}).

%% --------------------------------------------------------------------
%% Function: close/1 ->  ok | {error, Reason} 
%% Description: close file
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------
close(ClientWorkerPid) ->
	gen_server:call(?MODULE, {close,ClientWorkerPid}).
	
%% --------------------------------------------------------------------
%% Function: delete/1 ->  ok | {error, Reason} 
%% Description: delete file
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------
delete(FileName,UserName) ->
	gen_server:call(?MODULE, {delete, FileName,UserName}).	

%% --------------------------------------------------------------------
%% Function: copy/3 ->  ok | {error, Reason} 
%% Description: copy file and dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------
copy(SrcPath, DstPath, UserName) ->
	gen_server:call(?MODULE, {copy, SrcPath, DstPath, UserName}).		

%% --------------------------------------------------------------------
%% Function: move/3 ->  ok | {error, Reason} 
%% Description: move file and dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
move(SrcPath, DstPath, UserName) ->
	gen_server:call(?MODULE, {move, SrcPath, DstPath, UserName}).		
	
%% --------------------------------------------------------------------
%% Function: listdir/2 ->  list() | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
listdir(Dir, UserName) ->
	gen_server:call(?MODULE, {list, Dir, UserName}).	
	
%% --------------------------------------------------------------------
%% Function: mkdir/2 ->  ok | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
mkdir(Dir, UserName) ->
	gen_server:call(?MODULE, {mkdir, Dir, UserName}).	

%% --------------------------------------------------------------------
%% Function: chmod/4 ->  ok | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
chmod(FileName, UserName, UserType, CtrlACL) ->
	gen_server:call(?MODULE, {chmod, FileName, UserName, UserType, CtrlACL}).		
	
%% --------------------------------------------------------------------
%% Function: fileinfo/4 ->  ok | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
getfileinfo(FileName, UserName) ->
	gen_server:call(?MODULE, {getfileinfo, FileName, UserName}).	
	

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([]) ->
	%process_flag(trap_exit,true),
    {ok, []}.

handle_call({open, FileName, Mode, UserName}, _From, State) ->
	Reply=gen_server:start(client_worker, [FileName, Mode, UserName], []),
	{reply, Reply, State};	

handle_call({delete, FileName, UserName}, _From, State)  ->
	error_logger:info_msg("[~p, ~p]: delete ~p ~n", [?MODULE, ?LINE, FileName]),	
	Reply = gen_server:call(?META_SERVER, {delete, FileName, UserName}),					
	{reply, Reply, State};

handle_call({copy, SrcPath, DstPath, UserName}, {_From, _}, State) ->
	error_logger:info_msg("[~p, ~p]: cp src=~p dst=~p~n", [?MODULE, ?LINE, SrcPath,DstPath]),
    Reply = gen_server:call(?META_SERVER, {copy, SrcPath, DstPath, UserName}),	
    {reply, Reply, State};	

handle_call({move, SrcPath, DstPath, UserName}, {_From, _}, State) ->
	error_logger:info_msg("[~p, ~p]: mv src=~p dst=~p~n", [?MODULE, ?LINE, SrcPath,DstPath]),
    Reply = gen_server:call(?META_SERVER, {move, SrcPath, DstPath, UserName}),	
    {reply, Reply, State};	    

handle_call({list, Dir, UserName}, {_From, _}, State) ->
	error_logger:info_msg("[~p, ~p]: list dir=~p~n", [?MODULE, ?LINE, Dir]),
    Reply = gen_server:call(?META_SERVER, {list, Dir, UserName}),	
    {reply, Reply, State};

handle_call({mkdir, Dir, UserName}, {_From, _}, State) ->
	error_logger:info_msg("[~p, ~p]: make dir=~p~n", [?MODULE, ?LINE, Dir]),
    Reply = gen_server:call(?META_SERVER, {mkdir, Dir, UserName}),	
    {reply, Reply, State};    

handle_call({getfileinfo, FileName, UserName}, _From, State)  ->
	error_logger:info_msg("[~p, ~p]: fileinfo ~p ~n", [?MODULE, ?LINE, FileName]),	
	Reply = gen_server:call(?META_SERVER, {getfileinfo, FileName, UserName}),					
	{reply, Reply, State};

handle_call({chmod, FileName, UserName, UserType, CtrlACL}, {_From, _}, State) ->
	error_logger:info_msg("[~p, ~p]: chmod FileName:~p~n UserName:~p~n UserType:~p~n CtrlACL:~p~n", [?MODULE, ?LINE, [FileName, UserName, UserType, CtrlACL]]),
    Reply = gen_server:call(?META_SERVER, {chmod, FileName, UserName, UserType, CtrlACL}),	    
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State) ->
	error_logger:info_msg("[~p, ~p]: close client server ~p  since ~p~n", [?MODULE, ?LINE, self(),Reason]),	 
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

