%% Author: xuchuncong
%% Created: 2008-12-22
%% Description: TODO: Add description to metaDB

-module(acldb).
%-import(lists, [foreach/2]).
%-import(util,[for/3]).
%%
%% Include files
%%
-include("aclformat.hrl").
%-include("../include/egfs.hrl").
-include_lib("stdlib/include/qlc.hrl").
%%
%% Exported Functions
%%
-compile(export_all).

%%
%% API Functions
%%
start_mnesia()->
    case mnesia:create_schema([node()]) of
        ok ->
            mnesia:start(),
            mnesia:create_table(aclrecord, [{attributes, record_info(fields,aclrecord)},
                                      {disc_copies,[node()]}
                        ]),
            mnesia:create_table(grouprecord, [{attributes, record_info(fields, grouprecord)},
                                      {disc_copies,[node()]}
                        ]),
            mnesia:create_table(acllog, [{attributes, record_info(fields, acllog)},
                                      {disc_copies,[node()]}
                        ]),
            add_aclrecord_item("/", [{root, 7}], [], []),
            add_aclrecord_item("/public", [{root, 7}], [], [{other, 4}]),
            add_aclrecord_item("/private", [{root, 7}, {xcc, 7}], [{group1, 6}], []),
            add_aclrecord_item("/community", [{root, 7}, {hxm, 7}], [{group1, 6}], []),
            testacl:set_grouptab();
        _ ->
            LOG = #acllog{logtime = calendar:local_time(),logfunc="start_mnesia",logarg=[]},
            io:format("schema has already create"),
            mnesia:start(),
            mnesia:wait_for_tables([aclrecord, grouprecord, acllog], 5000),
            logF(LOG)
        end.
%%
%% Local Functions
%%
do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Val} = mnesia:transaction(F),
    Val.

example_tables() ->
    [ ].
clear_tables()->
    LOG = #acllog{logtime = calendar:local_time(),logfunc="cleart_tables/0",logarg=[]},
    logF(LOG),
    mnesia:clear_table(aclrecord),
    mnesia:clear_table(grouprecord),
    mnesia:clear_table(acllog).

reset_tables() ->
    LOG = #acllog{logtime = calendar:local_time(),logfunc="reset_tables/0",logarg=[]},
    logF(LOG),
    mnesia:clear_table(aclrecord),
    mnesia:clear_table(grouprecord),
    mnesia:clear_table(acllog),
    add_aclrecord_item("/", [{root, 7}], [], []),
    add_aclrecord_item("/public", [{root, 7}], [], [{other, 4}]),
    add_aclrecord_item("/private", [{root, 7}, {xcc, 7}], [{group1, 6}], []),
    add_aclrecord_item("/community", [{root, 7}, {hxm, 7}], [{group2, 6}], []),
    testacl:set_grouptab().

%aclrecord    {filename, userlist, grouplist, otherlist}
%add item
add_aclrecord_item(FileName, UserList, GroupList, OtherList) ->
    LOG = #acllog{logtime = calendar:local_time(), logfunc = "add_aclrecord_item/4",
	           logarg = [FileName, UserList, GroupList, OtherList]},
    logF(LOG),
    Row = #aclrecord{filename = FileName, userlist = UserList,
		     grouplist = GroupList, otherlist = OtherList},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).

%grouprecord    {groupname, grouplist}
%add item
add_grouprecord_item(GroupName, GroupList) ->
    LOG = #acllog{logtime = calendar:local_time(),logfunc="add_grouprecord_item/2",
		  logarg=[GroupName, GroupList]},
    logF(LOG),
    Row = #grouprecord{groupname = GroupName, grouplist = GroupList},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).


%% write a record 
%% 
write_to_db(X)->
    LOG = #acllog{logtime = calendar:local_time(),logfunc="write_to_db/1",logarg=[X]},
    logF(LOG),
    
    %io:format("inside write to db"),
    F = fun() ->
		mnesia:write(X)
	end,
    mnesia:transaction(F).





%% delete a record 
%%
delete_object_from_db(X)->
    LOG = #acllog{logtime = calendar:local_time(),logfunc="delete_object_from_db/1",logarg=[X]},
    logF(LOG),
    %io:format("inside delete from db"),
    F = fun() ->
                mnesia:delete_object(X)
        end,
    mnesia:transaction(F).


delete_from_db(X)->
    LOG = #acllog{logtime = calendar:local_time(),logfunc="delete_from_db/1",logarg=[X]},
    logF(LOG),
    %io:format("inside delete from db"),
    F = fun() ->
                mnesia:delete(X)
        end,
    mnesia:transaction(F).
  
delete_from_db(listrecord, [X|T])->
    delete_from_db(X),
    delete_from_db(listrecord,T);
delete_from_db(listrecord, [])->
	done.

delete_object_from_db(listrecord,[X|T])->
    delete_object_from_db(X),
    delete_object_from_db(listrecord,T);
delete_object_from_db(listrecord,[])->
	done.

%%------------------------------------------------------------------------------------------
%% select function
%% all kinds 
%%------------------------------------------------------------------------------------------ 
select_all_from_Table(T)->    
    do(qlc:q([
              X||X<-mnesia:table(T)
              ])).  %result [L]

select_all_from_grouptab(T)->
    do(qlc:q([
              X#grouprecord.groupname||X<-mnesia:table(T)
              ])).  %result [L]

select_all_from_aclrecord_prefix_is(FileName) ->    %result [L]
    do(qlc:q([
              X||X<-mnesia:table(aclrecord), lists:prefix(FileName, X#aclrecord.filename)
              ])).

select_all_from_aclrecord(FileName) ->    %result [L]
    do(qlc:q([
              X||X<-mnesia:table(aclrecord), X#aclrecord.filename =:= FileName
              ])).

select_all_from_grouprecord(GroupName)->
    do(qlc:q([
              X||X<-mnesia:table(grouprecord), X#grouprecord.groupname =:= GroupName
              ])).


%% powerfull log function
logF(X)->
    F = fun() ->
		mnesia:write(X)
	end,
mnesia:transaction(F).
