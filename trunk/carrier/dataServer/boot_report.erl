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
-export([get_all_chunkid/0, get_host_info/0, boot_report/0]).
%% -define(GM,{global, metagenserver}).


get_all_chunkid() ->
    chunk_db:get_all_chunkid().
    %% BChunklist = lists:map(fun(X) -> <<X:64>> end , Chunklist),
    %% io:format("[~p, ~p]: ~n", [?MODULE, ?LINE]),
    %% BChunklist.

get_proc_name() ->
    NodeName = node(),
    {ok, {?SERVER_NAME, NodeName}}.

get_free_space() ->
    io:format("[~p, ~p]: unimplemented!~n", [?MODULE, ?LINE]),
    {ok, 16106127360}.

get_total_space() ->
    io:format("[~p, ~p]: unimplemented!~n", [?MODULE, ?LINE]),
    {ok, 21474836480}.

get_host_info() ->
    {ok, Proc} = get_proc_name(),
    {ok, Host} = get_local_addr(),
    {ok, Free} = get_free_space(),
    {ok, Total} = get_total_space(),
    {ok, {hostinfo, {Proc, Host, Free, Total}}}.

boot_report()->
    {ok, HostInfo} = get_host_info(),
    io:format("[~p, ~p]: ~n", [?MODULE, ?LINE]),
    BChunklist = get_all_chunkid(),
    io:format("[~p, ~p]: ~n", [?MODULE, ?LINE]),
    case gen_server:call(?META_SERVER, {bootreport, HostInfo, BChunklist}) of
	{ok, OrphanChunkList} ->
	    io:format("[~p, ~p]: ~n", [?MODULE, ?LINE]),
	    OrphanChunkList;
	{error, Reason} ->
	    io:format("[~p, ~p]: ~n", [?MODULE, ?LINE]),
	    Reason;
	_Any ->
	    _Any
    
    end.
	  

