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
    mnesia:create_table(hostinfo, [{attributes, record_info(fields,hostinfo)},
                                      {disc_copies,[node()]}
                                     ]),
    
    mnesia:create_table(metalog, [{attributes, record_info(fields,metalog)},
                                      {disc_copies,[node()]}
                                     ]),
    
    mnesia:stop().

start_mnesia()->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="start_mnesia",logarg=[]},
    mnesia:start(),    
    mnesia:wait_for_tables([filemeta,filemeta_s,chunkmapping,hostinfo,metalog], 14000),
    logF(LOG).

%%
%% Local Functions
%%


do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Val} = mnesia:transaction(F),
    Val.

example_tables() ->
    [
     {hostinfo,data_server,abc,1000000,2000000}
    ].
clear_tables()->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="cleart_tables/0",logarg=[]},
    logF(LOG),

    mnesia:clear_table(filemeta),
    mnesia:clear_table(filemeta_s),
    mnesia:clear_table(hostinfo),
    mnesia:clear_table(chunkmapping).


reset_tables() ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="reset_tables/0",logarg=[]},
    logF(LOG),
    mnesia:clear_table(filemeta),
    mnesia:clear_table(chunkmapping),
    mnesia:clear_table(filemeta_s),
    mnesia:clear_table(hostinfo),

    F = fun() ->
		foreach(fun mnesia:write/1, example_tables())
		end,
    mnesia:transaction(F).

%filemeta    {fileid	client}
%add item
add_filemeta_item(Fileid, FileName) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="add_filemeta_item/2",logarg=[Fileid,FileName]},
    logF(LOG),
    Row = #filemeta{fileid=Fileid, filename=FileName, filesize=0, chunklist=[], 
                         createT=term_to_binary(erlang:localtime()), modifyT=term_to_binary(erlang:localtime()),acl="acl"},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).

%filemeta_s    {fileid	client}
%add item
add_filemeta_s_item(Fileid, FileName) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="add_filemeta_s_item/2",logarg=[Fileid,FileName]},
    logF(LOG),
    Row = #filemeta_s{fileid=Fileid, filename=FileName, filesize=0, chunklist=[], 
                         createT=term_to_binary(erlang:localtime()), modifyT=term_to_binary(erlang:localtime()),acl="acl"},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).



%% write a record 
%% 
write_to_db(X)->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="write_to_db/1",logarg=[X]},
    logF(LOG),
    
    %io:format("inside write to db"),
    F = fun() ->
		mnesia:write(X)
	end,
    mnesia:transaction(F).

%% delete a record 
%%
delete_from_db(X)->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="delete_from_db/1",logarg=[X]},
    logF(LOG),
    %io:format("inside delete from db"),
    F = fun() ->
                mnesia:delete(X)
        end,
    mnesia:transaction(F).
  

%%------------------------------------------------------------------------------------------
%% select function
%% all kinds 
%%------------------------------------------------------------------------------------------ 
select_all_from_Table(T)->    
    do(qlc:q([
              X||X<-mnesia:table(T)
              ])).  %result [L]

select_all_from_filemeta(FileID) ->    %result [L]
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
    LOG = #metalog{logtime = calendar:local_time(),logfunc="select_fileid_from_filemeta/1",logarg=[FileName]},
    logF(LOG),
    
    do(qlc:q([X#filemeta.fileid || X <- mnesia:table(filemeta),
                                   X#filemeta.filename =:= FileName                                   
                                   ])).   %result [L]

select_fileid_from_filemeta_s(FileName) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="select_fileid_from_filemeta_s/1",logarg=[FileName]},
    logF(LOG),
    do(qlc:q([X#filemeta_s.fileid || X <- mnesia:table(filemeta_s),
                                   X#filemeta_s.filename =:= FileName                                   
                                   ])).   %result [L]

select_nodeip_from_chunkmapping(ChunkID) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="select_nodeip_from_chunkmapping",logarg=[ChunkID]},
    logF(LOG),
    do(qlc:q([X#chunkmapping.chunklocations || X <- mnesia:table(chunkmapping),
                                   X#chunkmapping.chunkid =:= ChunkID
             ])).   %result [L]

%clear chunks from filemeta
reset_file_from_filemeta(Fileid) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="reset_file_from_filemeta",logarg=[Fileid]},
    logF(LOG),
    [{filemeta, FileID, FileName, _, _, TimeCreated, _, ACL }] =
    do(qlc:q([X || X <- mnesia:table(filemeta),
                                   X#filemeta.fileid =:= Fileid                                   
                                   ])),

	Row = {filemeta, FileID, FileName, 0, [], TimeCreated, term_to_binary(erlang:localtime()), ACL },
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).


%% powerfull log function
log(X,Y)->
    F = fun() ->
		mnesia:write(X)
	end,
mnesia:transaction(F).

%% --------------------------------------------------------------------
%% Function: 
%% Description: 
%% Returns: 
%% --------------------------------------------------------------------


%% --------------------------------------------------------------------
%% Function: reset_file_from_filemeta/1
%% Description: 
%% Argument: Fileid  @type <<binary:64>>
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------


%%		TODO:  log of all function. 
