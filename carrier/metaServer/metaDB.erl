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


%tables.
-record(filemetaTable, {fileid, filename, filesize, chunklist, createT, modifyT, acl}).
-record(chunkmappingTable, {chunkid, chunklocations}).
%record current active client-metaserver sesseions
-record(clientinfoTable, {clientid, modes}).   % maybe fileid?
-record(filesessionTable, {fileid, client}).


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

%-record(filemetaTable, {fileid, filename, filesize, chunklist, createT, modifyT, acl}).
%-record(chunkmappingTable, {chunkid, chunklocations}).

example_tables() ->
    [%% The filemeta table
     %			fileid,  filename, 			filesize, chunklist, createT, modifyT, acl
     {filemeta, 0,   "egfs://e:/copy/test.txt",3,[0],"today,Dec,12","yestoday,Dec,11","acl"},
     {filemeta, 1,   "egfs://e:/copy/test.txt",3,[1],"today,Dec,12","yestoday,Dec,11","acl"},
     
     %% The chunkmapping table
     {chunkmapping,0,[localhost]},
     {chunkmapping,1,[localhost]}
    ].


add_shop_item(Name, Quantity, Cost) ->
    Row = #shop{item=Name, quantity=Quantity, cost=Cost},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).

reset_tables() ->
    mnesia:clear_table(filemeta),
    mnesia:clear_table(chunkmapping),
    F = fun() ->
		foreach(fun mnesia:write/1, example_tables())
	end,
    mnesia:transaction(F).


%filesession    {fileid	client}
add_filesession_item(Fileid, Client) ->
    Row = #shop{fileid=Fileid, client=Client},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).

%while remove . we shall use primary key(first element in mnesia.)
remove_filesession_item(Fileid) ->
    Oid = {filesession, Fileid},
    F = fun() ->
		mnesia:delete(Oid)
	end,
    mnesia:transaction(F).






