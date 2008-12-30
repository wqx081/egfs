%%%-------------------------------------------------------------------
%%% File    : boot_report.erl
%%% Author  : pp <>
%%% Description : 
%%%
%%% Created : 29 Dec 2008 by pp <>
%%%-------------------------------------------------------------------
-module(boot_report).
-export([get_all_chunkid/0, get_host_info/1, boot_report/1]).
-define(GM,{global, metagenserver}).


get_all_chunkid() ->
    Chunklist = chunk_meta_db:get_all_chunkid(),
    BChunklist = lists:map(fun(X) -> <<X:64>> end , Chunklist),
    BChunklist.

get_host_info(Tmpip) ->
    [Hostinfo] = chunk_meta_db:get_host_info_by_ip(Tmpip),
%%    Hostinfo = {hostinfo, {A,B,C,D}},
    Hostinfo.

get_host_info(

boot_report(Tmpip)->
    case gen_server:call(?GM, {bootreport, get_host_info(Tmpip), get_all_chunkid()}) of
	{ok, OrphanChunkList} ->
	    OrphanChunkList;
	{error, Reason} ->
	    Reason;
	_Any ->
	    _Any
    
    end.
	  

