%%%-------------------------------------------------------------------
%%% File    : grouptab.erl
%%% Author  : 
%%% Description : the access control list functions
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(grouptab).
%-include("../include/egfs.hrl").
-export([set_grouptab/0]).
-compile(export_all).

%%----------------------------------------------------------------------
%% client_api,can be used for application progamming
%%----------------------------------------------------------------------

%% --------------------------------------------------------------------
%% Function: new_grouptab/0
%% Description: to new a ets table
%% Returns: tid()                  |
%% --------------------------------------------------------------------
new_grouptab() ->
    ets:new(tabgroup, []).

%% --------------------------------------------------------------------
%% Function: addgroup_to_grouptab/2
%% Description: add a new group to ets table
%% Returns: true                        |
%%          {ok, group_has_exist}       |
%% --------------------------------------------------------------------
addgroup_to_grouptab(Tabgroup, GroupName) ->
    case ets:lookup(Tabgroup, GroupName) of
	[] ->
	    ets:insert(Tabgroup, {GroupName, []});
	[_Object] ->
	    {ok, group_has_exist}
    end.

%% --------------------------------------------------------------------
%% Function: delgroup_from_grouptab/2
%% Description:delete a group from grouptab
%% Returns: true                         |
%%          {ok, no_this_group}          |
%% --------------------------------------------------------------------
delgroup_from_grouptab(Tabgroup, GroupName) ->
    case ets:lookup(Tabgroup, GroupName) of
	[] ->
	    {ok, no_this_group};
	[Object] ->
	    ets:delete_object(Tabgroup, Object)
    end.

%% --------------------------------------------------------------------
%% Function: adduser_to__grouptab/2
%% Description: add a new user to group member list
%% Returns: {ok, NewGroupMemberList}     |
%%	    {ok, no_this_group}          |
%% --------------------------------------------------------------------
adduser_to_group(Tabgroup, GroupName, UserName) ->
    case ets:lookup(Tabgroup, GroupName) of
	[] ->
	    {ok, no_this_group};
	[Object] ->
	    {_GroupName1, GroupMemberList} = Object,
	    case lists:member(UserName, GroupMemberList) of
		true ->
		    {ok, user_has_exist_in_this_group};
		false ->
		    NewGroupMemberList = [UserName|GroupMemberList],
		    ets:insert(Tabgroup, {GroupName, NewGroupMemberList}),
		    {ok, NewGroupMemberList}
	    end
    end.

%% --------------------------------------------------------------------
%% Function: deluser_from__grouptab/2
%% Description: delete a user from group member list
%% Returns: {ok, NewGroupMemberList}             |
%%          {ok, no_this_group}                  |
%%          {ok, user_not_exist_in_this_group}}  |
%% --------------------------------------------------------------------
deluser_from_group(Tabgroup, GroupName, UserName) ->
    case ets:lookup(Tabgroup, GroupName) of
	[] ->
	    {ok, no_this_group};
	[Object] ->
	    {_GroupName1, GroupMemberList} = Object,
	    case lists:member(UserName, GroupMemberList) of
		true ->
		    NewGroupMemberList = lists:delete(UserName, GroupMemberList),
		    ets:insert(Tabgroup, {GroupName, NewGroupMemberList}),
		    {ok, NewGroupMemberList};
		false ->
		    {ok, user_not_exist_in_this_group}
	    end
    end.

%% --------------------------------------------------------------------
%% Function: getgroups_from_grouptab/1
%% Description: transfrom grouptab to a group name list
%% Returns: {ok, List}                           |
%% --------------------------------------------------------------------
getgroups_from_grouptab(Tabgroup) ->
    Object = ets:tab2list(Tabgroup),
    grouplist_to_groupnamelist(Object, []).

grouplist_to_groupnamelist(Object, List) when Object =:= [] ->
    {ok, List};
grouplist_to_groupnamelist(Object, List) ->
    [{GroupName, _GroupMemberList}|T] = Object,
    ListTemp = [GroupName|List],
    io:format("[Group, ~p]:ListTemp is:~p~n", [?LINE, ListTemp]),
    grouplist_to_groupnamelist(T, ListTemp).

%% --------------------------------------------------------------------
%% Function: set_grouptab/0
%% Description: new a grouptab and creat groups(user1\user2\user3)
%% Returns:tid()                           |
%% --------------------------------------------------------------------
set_grouptab() ->
    Tabgroup = new_grouptab(),
    addgroup_to_grouptab(Tabgroup, user1),
    addgroup_to_grouptab(Tabgroup, user2),
    addgroup_to_grouptab(Tabgroup, user3),
    adduser_to_group(Tabgroup, user1, hxm),
    adduser_to_group(Tabgroup, user1, llk),
    adduser_to_group(Tabgroup, user1, xpz),
    adduser_to_group(Tabgroup, user2, xcc),
    adduser_to_group(Tabgroup, user2, lsb),
    adduser_to_group(Tabgroup, user2, pyy),
    adduser_to_group(Tabgroup, user3, zyb),
    adduser_to_group(Tabgroup, user3, njr),
    adduser_to_group(Tabgroup, user3, zm),
    ets:i(Tabgroup),
    Tabgroup.
