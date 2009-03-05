%%%-------------------------------------------------------------------
%%% File    : meta_hosts.erl
%%% Author  : Xiaomeng Huang
%%% Description : Host server for all data node
%%%
%%% Created :  31 Jan 2009 by Xiaomeng Huang
%%%-------------------------------------------------------------------
-module(meta_hosts).
-behaviour(gen_server).
-include("../include/header.hrl").
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    error_logger:info_msg("[~p, ~p]: hostserver starting ~n", [?MODULE, ?LINE]),
    gen_server:start_link(?HOST_SERVER, ?MODULE, [], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([]) ->
	process_flag(trap_exit,false),
    {ok, []}.

handle_call({register_dataserver, HostName, FreeSpace, TotalSpace, Status}, {From, _}, State) ->
    error_logger:info_msg("[~p, ~p]: register dataserver ~p request~n", [?MODULE, ?LINE, HostName]),
 	Reply = meta_db:add_hostinfo_item(HostName, FreeSpace, TotalSpace, Status,From),
	{reply, Reply, State};

handle_call({allocate_dataserver}, {_From, _}, State) ->
    error_logger:info_msg("[~p, ~p]: allocate dataserver request~n", [?MODULE, ?LINE]),
	case meta_db:select_random_one_from_hostinfo() of
		[] ->
			error_logger:error_msg("[~p, ~p]: seek dataserver failed~n", [?MODULE, ?LINE]),
			{reply, {error, "seek dataserver failed"}, State};
		[SelectedHost] ->
			{reply, {ok, SelectedHost}, State}
	end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

