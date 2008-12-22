%% Author: zyb
%% Created: 2008-12-22
%% Description: TODO: Add description to metaDB

-module(metaDB).

%%
%% Include files
%%
-include("metaformat.hrl").

%%
%% Exported Functions
%%
%-export([]).
-compile(export_all).



%-export([do_this_once/0, start_start_mnesia/0]).
-record(filemetaTable, {fileid, filename, filesize, chunklist, createT, modifyT, acl}).
-record(chunkmappingTable, {chunkid, chunklocations}).
%record current active client-metaserver sesseions
-record(clientinfoTable, {clientid, modes}).
-record(filesessionTable, {fileid, clientlist}).


%%
%% API Functions
%%

do_this_once() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(filemeta, [{attributes, record_info(fields, filemetaTable)}]),
    mnesia:create_table(chunkmapping, [{attributes, record_info(fields, chunkmappingTable)}]),
    mnesia:create_table(clientinfo, [{attributes, record_info(fields, clientinfoTable)}]),
    mnesia:create_table(filesession, [{attributes, record_info(fields, filesessionTable)}]),
    mnesia:stop().

start_mnesia()->
    mnesia:start(),
    mnesia:wait_for_tables([filemeta, chunkmapping,clientinfo,filesession], 20000).


%%
%% Local Functions
%%


%% SQL equivalent
%%  SELECT * FROM shop;

demo(select_filemeta) ->
    do(qlc:q([X || X <- mnesia:table(filemeta)]));

demo(select_chunkmapping) ->
    do(qlc:q([X || X <- mnesia:table(chunkmapping)]));
demo(select_clientinfo) ->
    do(qlc:q([X || X <- mnesia:table(clientinfo)]));
demo(select_filesession) ->
    do(qlc:q([X || X <- mnesia:table(filesession)]));



%% SQL equivalent
%%  SELECT item, quantity FROM shop;

demo(select_some) ->
    do(qlc:q([{X#filemeta.fileid, X#filemeta.filename} || X <- mnesia:table(filemeta)]));

%% SQL equivalent
%%   SELECT shop.item FROM shop
%%   WHERE  shop.quantity < 250;

demo(reorder) ->
    do(qlc:q([X#filemeta.filename || X <- mnesia:table(filemeta),
			     X#filemeta.fileid < 250
				]));



