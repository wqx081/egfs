%%%-------------------------------------------------------------------
%%% File    : boot_report.erl
%%% Author  : pp <>
%%% Description : 
%%%
%%% Created : 29 Dec 2008 by pp <>
%%%-------------------------------------------------------------------
-module(boot_report).
-export([get_all_chunkid/0, get_host_info/0, boot_report/0]).
-define(GM,{global, metagenserver}).


get_all_chunkid() ->
    Chunklist = chunk_meta_db:get_all_chunkid(),
    BChunklist = lists:map(fun(X) -> <<X:64>> end , Chunklist),
    BChunklist.



boot_report()->
    case gen_server:call(?GM, {bootreport, get_host_info(), get_all_chunkid()}) of
	{ok, OrphanChunkList} ->
	    OrphanChunkList;
	{error, Reason} ->
	    Reason;
	_Any ->
	    _Any
    
    end.
	  

