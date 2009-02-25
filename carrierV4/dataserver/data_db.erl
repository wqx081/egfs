%%%-------------------------------------------------------------------
%%% File    : data_db.erl
%%% Author  : Xiaomeng Huang
%%% Description : Metadata database on Mnesia 
%%%
%%% Created :  30 Jan 2007 by Xiaomeng Huang 
%%%-------------------------------------------------------------------
-module(data_db).
-include("../include/header.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

%%====================================================================
%% API
%%====================================================================
do_this_once() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(chunkmeta,	[{type, set}, {attributes, record_info(fields, chunkmeta)},     {disc_copies,[node()]}]),
    mnesia:stop().

clear_tables() ->
    mnesia:clear_table(chunkmeta).

start() ->
    mnesia:start(),    
    mnesia:wait_for_tables([chunkmeta], 20000).

do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Val} = mnesia:transaction(F),
    Val.

%%====================================================================
%% select item from table
%%====================================================================
select_item_from_chunkmeta_id(ChunkID) ->    
    do(qlc:q([X||X<-mnesia:table(filemeta),X#chunkmeta.chunkid =:= ChunkID])).

%%====================================================================
%% add item into table
%%====================================================================
add_chunkmeta_item(ChunkID, MD5) ->
   	Row = #chunkmeta{chunkid=ChunkID, md5=MD5},
	F = fun() ->
		mnesia:write(Row)
	end,
    {atomic, Val} = mnesia:transaction(F),
	Val.

%%====================================================================
%% delete item from table
%%====================================================================
delete_chunkmeta_item(ChunkID) ->
	Oid = {chunkmeta, ChunkID},
	F = fun() ->
		mnesia:delete(Oid)
	end,
	{atomic, Val} = mnesia:transaction(F),
	Val.
	

