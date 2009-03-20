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
clear_tables() ->
	mnesia:start(),
    mnesia:clear_table(chunkmeta),
    mnesia:stop().

start() ->
	case mnesia:create_schema([node()]) of
		ok ->
			mnesia:start(),
			mnesia:create_table(chunkmeta,	[{type, set}, {attributes, record_info(fields, chunkmeta)}, {disc_copies,[node()]}]);
		_ ->
			mnesia:start()			
	end,
	mnesia:wait_for_tables([chunkmeta], 20000).


do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Val} = mnesia:transaction(F),
    Val.

%%====================================================================
%% select item from table
%%====================================================================
select_item_from_chunkmeta_id(ChunkID) ->    
    do(qlc:q([X||X<-mnesia:table(chunkmeta),X#chunkmeta.chunkid =:= ChunkID])).

select_md5_from_chunkmeta_id(ChunkID) ->    
    do(qlc:q([X#chunkmeta.md5||X<-mnesia:table(chunkmeta),X#chunkmeta.chunkid =:= ChunkID])).
    
select_chunklist_from_chunkmeta() ->    
    do(qlc:q([X#chunkmeta.chunkid||X<-mnesia:table(chunkmeta)])). 

is_exsit_in_chunkmeta(ChunkID, MD5) ->    
	R = do(qlc:q([X||X<-mnesia:table(chunkmeta),
    			 X#chunkmeta.chunkid =:= ChunkID,
    			 X#chunkmeta.md5 =:= MD5])),
    case R of
		[] ->
			false;
		[_Any] ->
			true
	end.	
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


