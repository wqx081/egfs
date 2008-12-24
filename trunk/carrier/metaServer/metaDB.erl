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

%%
%% API Functions
%%

do_this_once() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(filemeta, 	%table name 
                        [			
                         {attributes, record_info(fields, filemeta)},%table content
                         {disc_copies,[node()]}                         
                        ]
                       ),
    mnesia:create_table(filemeta_s, 	%table name 
                        [			
                         {attributes, record_info(fields, filemeta_s)},%table content
                         {disc_copies,[node()]}                         
                        ]
                       ),
    mnesia:create_table(chunkmapping, [{attributes, record_info(fields, chunkmapping)},
                                       {disc_copies,[node()]}
                                      ]),
%%     mnesia:create_table(clientinfo, [{attributes, record_info(fields, clientinfoTable)},
%%                                      {disc_copies,[node()]}
%%                                     ]),
%%     mnesia:create_table(filesession, [{attributes, record_info(fields, filesession)},
%%                                       {disc_copies,[node()]}
%%                                      ]),
    mnesia:create_table(hostinfo, [{attributes, record_info(fields, hostinfo)},
                                      {disc_copies,[node()]}
                                     ]),
    
    mnesia:stop().

start_mnesia()->
    mnesia:start(),
    mnesia:wait_for_tables([filemeta,filemeta_s,chunkmapping,hostinfo], 30000).


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

example_tables() ->
    [%% The filemeta table
     %			fileid,  filename, 			filesize, chunklist, createT, modifyT, acl
     {filemeta, 0,   "egfs://e:/copy/test.txt",3,[0],"today,Dec,12","yestoday,Dec,11","acl"},
     {filemeta, 1,   "egfs://e:/copy/test.txt",3,[1],"today,Dec,12","yestoday,Dec,11","acl"},
     
     %% The chunkmapping table
%%      {chunkmapping,0,[localhost]},
%%      {chunkmapping,1,[localhost]}
     
     
     {hostinfo,data_server,abc,1000000,2000000}
     
    ].

example_table_filemeta(X)->
%    {filemeta, X,   "e:/copy/test",3,[X],"today,Dec,12","yestoday,Dec,11","acl"}.
#filemeta{fileid=X,filename=["e:/copy/test",X],filesize=3,chunklist=[X],createT="today,Dec,12",modifyT="yestoday,Dec,11",acl="acl"}.

clear_tables()->
    mnesia:clear_table(filemeta),
    mnesia:clear_table(filemeta_s),
    mnesia:clear_table(hostinfo),
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
    mnesia:clear_table(filemeta_s),
	mnesia:clear_table(clientinfo),

    F = fun() ->
		foreach(fun mnesia:write/1, example_tables())
		end,
    mnesia:transaction(F).

insert_ten_thousand(X)->
    mnesia:clear_table(filemeta),
           
    util:for(1,X,
             fun(I)->insert_filemeta_sample(I) end
			).
          

insert_filemeta_sample(X)->
    Row = example_table_filemeta(X),
    F = fun()->
                mnesia:write(Row)
        end,
    mnesia:transaction(F).


%filemeta    {fileid	client}
%add item
add_filemeta_item(Fileid, FileName) ->
    Row = #filemeta{fileid=Fileid, filename=FileName, filesize=0, chunklist=[], 
                         createT=term_to_binary(erlang:localtime()), modifyT=term_to_binary(erlang:localtime()),acl="acl"},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).

%filemeta_s    {fileid	client}
%add item
add_filemeta_s_item(Fileid, FileName) ->
    Row = #filemeta_s{fileid=Fileid, filename=FileName, filesize=0, chunklist=[], 
                         createT=term_to_binary(erlang:localtime()), modifyT=term_to_binary(erlang:localtime()),acl="acl"},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).

%filesession    {fileid	client}
%add item
add_filesession_item(Fileid, Client) ->
    Row = #filesession{fileid=Fileid, client=Client},
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


%%------------------------------------------------------------------------------------------
%% select function
%% all kinds 
%%------------------------------------------------------------------------------------------ 
select_all_from_anyTable(T)->
    do(qlc:q([
              X||X<-mnesia:table(T)
              ])).  %result [L]

%look up.
select_from_filesession(Fileid) ->    %result [L]
    do(qlc:q([
              X||X<-mnesia:table(filesession),X#filesession.fileid =:= Fileid
              ])).

select_from_filemeta(FileID) ->    %result [L]
    do(qlc:q([
              X||X<-mnesia:table(filemeta),X#filemeta.fileid =:= FileID
              ])).

select_all_from_filemeta_s(FileID)->
    do(qlc:q([
              X||X<-mnesia:table(filemeta_s),X#filemeta_s.fileid =:= FileID
              ])).

%FileName - > fileid
% @spec select_fileid_from_filemeta(FileName) ->  fileid
% fileid -> binary().

select_fileid_from_filemeta(FileName) ->
    do(qlc:q([X#filemeta.fileid || X <- mnesia:table(filemeta),
                                   X#filemeta.filename =:= FileName                                   
                                   ])).   %result [L]


select_fileid_from_filemeta_s(FileName) ->
    do(qlc:q([X#filemeta_s.fileid || X <- mnesia:table(filemeta_s),
                                   X#filemeta_s.filename =:= FileName                                   
                                   ])).   %result [L]


select_test(FileID) ->
    do(qlc:q([X#filemeta.fileid || X <- mnesia:table(filemeta),
                                   X#filemeta.fileid =:= FileID                                   
                                   ])).   %result [L]

select_nodeip_from_chunkmapping(ChunkID) ->
    do(qlc:q([X#chunkmapping.chunklocations || X <- mnesia:table(chunkmapping),
                                   X#chunkmapping.chunkid =:= ChunkID
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

% look_up_filesession(FileID, ClientID) -> w | r | a
% FileID = binary
% ClientID = binary
look_up_filesession(FileID, ClientID) ->
	case select_from_filesession(FileID) of
		% file hasn't been opened yet, so open operation is permitted.
		[clientinfo, [ClientInfo]] when (ClientInfo#clientinfo.clientid =:= ClientID) -> 
				ClientInfo#clientinfo.modes,
                {ok, <<FileID:64>>};   
		% file has been opened by other processes, so open operation is denied.
		[_] -> {error, "opened already"}
	end.

%% demo(reorder) ->
%%     do(qlc:q([X#filemeta.filename || X <- mnesia:table(filemeta),
%% 			     X#filemeta.fileid < 250
%% 				]));


