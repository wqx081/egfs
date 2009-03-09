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
			list_dir/1,
			open/3, 
			write/2, 
			read/2, 
			close/1, 
			delete/2]).
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
%% Function: list(string(),Mode) -> {ok, FileNames} | {error, Reason} 
%% Description: list sub files and directories
%%--------------------------------------------------------------------------------
list_dir(FileName) ->
	gen_server:call(?MODULE, {listdir, FileName}).

%%--------------------------------------------------------------------------------
%% Function: open(string(),Mode) -> {ok, ClientWorkerPid} | {error, Reason} 
%% 			 Mode = r | w 
%% Description: open a file according to the filename and open mode
%%--------------------------------------------------------------------------------
open(FileName, Mode, UserName) ->
	gen_server:call(?MODULE, {open, FileName, Mode, UserName}).

%% --------------------------------------------------------------------
%% Function: write(binary()) ->  {ok, FileContext} | {error, Reason} 
%% Description: write Bytes to dataserver
%% --------------------------------------------------------------------
write(ClientWorkerPid, Bytes) ->
	gen_server:call(?MODULE, {write, ClientWorkerPid, Bytes},120000).

%% --------------------------------------------------------------------
%% Function: read(int()) ->  {ok, FileContext, Data} | {eof, FileContext} | {error, Reason} 
%% Description: read Number Bytes from Location offset
%% --------------------------------------------------------------------
read(ClientWorkerPid, Number) ->
	gen_server:call(?MODULE, {read, ClientWorkerPid, Number}, 120000).
	
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
	
%%====================================================================
%% gen_server callbacks
%%====================================================================
init([]) ->
	%process_flag(trap_exit,true),
    {ok, []}.

handle_call({open, FileName, Mode, UserName}, _From, State) ->
	{ok, ClientWorkerPid}=gen_server:start(client_worker, [], []),
	Reply = gen_server:call(ClientWorkerPid,{open, FileName, Mode, UserName}),
	{reply, Reply, State};	


handle_call({write, ClientWorkerPid, Bytes}, _From, State) ->
	Reply = gen_server:call(ClientWorkerPid,{write, Bytes},120000),
	{reply, Reply, State};	
	
handle_call({read, ClientWorkerPid, Number}, _From, State) ->
	Reply = gen_server:call(ClientWorkerPid,{read, Number},120000),
	{reply, Reply, State};	

handle_call({close, ClientWorkerPid}, _From, State) ->
	Reply = gen_server:call(ClientWorkerPid,{close}),
	{reply, Reply, State};	

handle_call({delete, FileName, UserName}, _From, State)  ->
	error_logger:info_msg("[~p, ~p]: delete ~p ~n", [?MODULE, ?LINE, FileName]),	
	Reply = gen_server:cast(?META_SERVER, {delete, FileName, UserName}),					
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

