%%%-------------------------------------------------------------------
%%% File    : boot_report.erl
%%% Author  : pp <>
%%% Description : 
%%%
%%% Created : 29 Dec 2008 by pp <>
%%%-------------------------------------------------------------------
-module(boot_report).
-include("../include/egfs.hrl").
-include("chunk_info.hrl").
-import(toolkit, [get_local_addr/0]).
-export([get_all_chunk_id/0, get_host_info/0, boot_report/0, get_proc_name/0]).

get_all_chunk_id() ->
    chunk_db:get_all_chunk_id().
    

get_proc_name() ->
    NodeName = node(),
    {ok, {?SERVER_NAME, NodeName}}.

get_free_space() ->
    {ok, 16106127360}.


get_total_space() ->
    {ok, 21474836480}.

get_host_info() ->
    {ok, Proc} = get_proc_name(),
    {ok, Host} = get_local_addr(),
    {ok, Free} = get_free_space(),
    {ok, Total} = get_total_space(),    
    {ok, {hostinfo, Proc, Host, Free, Total, {0, 0}}}.

boot_report()->
    {ok, HostInfo} = get_host_info(),
    BChunklist = get_all_chunk_id(),
    io:format("[~p, ~p ] ~n", [?MODULE, ?LINE]),
    HostInfo,
    BChunklist,
    case  gen_server:call(?META_SERVER, {bootreport, HostInfo, BChunklist}) of
	{ok, OrphanChunkList} ->
	    chunk_garbage_collect:collect(OrphanChunkList);
	_Any ->
	    _Any
    end.

    %%io:format("[~p, ~p ] ~n", [?MODULE, ?LINE]),





	  

