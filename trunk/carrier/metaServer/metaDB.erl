%% Author: zyb
%% Created: 2008-12-22
%% Description: TODO: Add description to metaDB

-module(metaDB).
-import(lists, [foreach/2]).
-import(util,[for/3]).

%%
%% Include files
%%
-include("metaformat.hrl").
-include_lib("stdlib/include/qlc.hrl").
%%
%% Exported Functions
%%
%-export([]).
-compile(export_all).

%tables  record to create table.
-record(filemetaTable, {fileid, filename, filesize, chunklist, createT, modifyT, acl}).
-record(chunkmappingTable, {chunkid, chunklocations}).
-record(clientinfo, {clientid, modes}).   % maybe fileid?
-record(filesessionTable, {fileid, client}).


%%
%% API Functions
%%

do_this_once() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(filemeta, 	%table name 
                        [			
                         {attributes, record_info(fields, filemetaTable)},%table content
                         {disc_copies,[node()]}                         
                        ]
                       ),
    mnesia:create_table(chunkmapping, [{attributes, record_info(fields, chunkmappingTable)},
                                       {disc_copies,[node()]}
                                      ]),
%%     mnesia:create_table(clientinfo, [{attributes, record_info(fields, clientinfoTable)},
%%                                      {disc_copies,[node()]}
%%                                     ]),
    mnesia:create_table(filesession, [{attributes, record_info(fields, filesessionTable)},
                                      {disc_copies,[node()]}
                                     ]),
    mnesia:stop().

start_mnesia()->
    mnesia:start(),
    mnesia:wait_for_tables([filemeta, chunkmapping,filesession], 30000).


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
    do(qlc:q([{X#filemeta.fileid, X#filemeta.filename} || X <- mnesia:table(filemeta)])).

do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Val} = mnesia:transaction(F),
    Val.

%% SQL equivalent
%%   SELECT shop.item FROM shop
%%   WHERE  shop.quantity < 250;

%% demo(reorder) ->
%%     do(qlc:q([X#filemeta.filename || X <- mnesia:table(filemeta),
%% 			     X#filemeta.fileid < 250
%% 				]));

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

example_table_filemeta(X)->
%    {filemeta, X,   "e:/copy/test",3,[X],"today,Dec,12","yestoday,Dec,11","acl"}.
#filemetaTable{fileid=X,filename=["e:/copy/test",X],filesize=3,chunklist=[X],createT="today,Dec,12",modifyT="yestoday,Dec,11",acl="acl"}.

clear_tables()->
    mnesia:clear_table(filemeta),
    mnesia:clear_table(filesession),
    mnesia:clear_table(chunkmapping).


%% add_shop_item(Name, Quantity, Cost) ->
%%     Row = #shop{item=Name, quantity=Quantity, cost=Cost},
%%     F = fun() ->
%% 		mnesia:write(Row)
%% 	end,
%%     mnesia:transaction(F).

reset_tables() ->
    mnesia:clear_table(filemeta),
    mnesia:clear_table(chunkmapping),
    F = fun() ->
		foreach(fun mnesia:write/1, example_tables())
	end,
    mnesia:transaction(F).

insert_ten_thousand()->
    mnesia:clear_table(filemeta),
    F=fun()->              
              util:for(1,10,fun(I)->mnesia:write(example_table_filemeta(I))end)
    end.      


for_test() ->
   
    
    util:for(1,10,fun(I)->I*I end ).

%filemeta    {fileid	client}
%add item
add_filemeta_item(Fileid, FileName) ->
    Row = #filemetaTable{fileid=Fileid, filename=FileName, filesize=0, chunklist=[], 
                         createT=term_to_binary(erlang:localtime(), modifyT=term_to_binary(erlang:localtime(), "acl"},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).

%filesession    {fileid	client}
%add item
add_filesession_item(Fileid, Client) ->
    Row = #filesessionTable{fileid=Fileid, client=Client},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).

%remove   while remove . we shall use primary key(first element in mnesia.)
remove_filesession_item(Fileid) ->
    Oid = {filesession, Fileid},
    F = fun() ->
		mnesia:delete(Oid)
	end,
    mnesia:transaction(F).


%look up.
select_from_filesession(Fileid) ->    %result [L]
    do(qlc:q([
              X||X<-mnesia:table(filesession),X#filesession.fileid =:= Fileid
              ])).


select_allfrom_filesession() ->
    do(qlc:q([X || X <- mnesia:table(filesession)])).   %result [L]

%look up.
select_allfrom_chunkmapping() ->
    do(qlc:q([X || X <- mnesia:table(chunkmapping)])).   %result [L]

%look up.
select_allfrom_filemeta() ->    
    do(qlc:q([X || X <- mnesia:table(filemeta)])).   %result [L]

select_from_filemeta(S) ->    %result [L]
    do(qlc:q([
              X||X<-mnesia:table(filemeta),X#filemeta.fileid =:= S
              ])).

%FileName - > fileid
select_fileid_from_filemeta(FileName) ->
    do(qlc:q([X#filemeta.fileid || X <- mnesia:table(filemeta),
                                   X#filemeta.filename =:= FileName
                                   
                                   ])).   %result [L]

%clear chunks from filemeta
reset_file_from_filemeta(Fileid) ->
    [{filemeta, FileID, FileName, _, _, TimeCreated, _, ACL }] =
    do(qlc:q([X || X <- mnesia:table(filemeta),
                                   X#filemeta.fileid =:= Fileid                                   
                                   ])),

	Row = {filemeta, FileID, FileName, 0, [], TimeCreated, term_to_binary(erlang:localtime()), ACL },
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).


%% demo(reorder) ->
%%     do(qlc:q([X#filemeta.filename || X <- mnesia:table(filemeta),
%% 			     X#filemeta.fileid < 250
%% 				]));


