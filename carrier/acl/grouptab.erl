%%%-------------------------------------------------------------------
%%% File    : grouptab.erl
%%% Author  : 
%%% Description : the access control list functions
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(grouptab).
%-include("../include/egfs.hrl").
%-export([set_grouptab/0]).
-compile(export_all).

%% --------------------------------------------------------------------
%% Function: addgroup_to_grouptab/2
%% Description: add a new group to ets table
%% Returns: true                        |
%%          {ok, group_has_exist}       |
%% --------------------------------------------------------------------
addgroup_to_grouptab(GroupName) ->
    case acldb:select_all_from_grouprecord(GroupName) of
	[] ->
        acldb:add_grouprecord_item(GroupName, []),
        ok;
	    %ets:insert(Tabgroup, {GroupName, []});
	[_Object] ->
	    {error, group_has_exist}
    end.

%% --------------------------------------------------------------------
%% Function: delgroup_from_grouptab/2
%% Description:delete a group from grouptab
%% Returns: true                         |
%%          {ok, no_this_group}          |
%% --------------------------------------------------------------------
delgroup_from_grouptab(GroupName) ->
    case acldb:select_all_from_grouprecord(GroupName) of
	[] ->
	    {error, no_this_group};
	[Object] ->
        acldb:delete_object_from_db(Object),
        ok
	    %ets:delete_object(Tabgroup, Object)
    end.

%% --------------------------------------------------------------------
%% Function: adduser_to__grouptab/2
%% Description: add a new user to group member list
%% Returns: {ok, NewGroupMemberList}     |
%%	    {ok, no_this_group}          |
%% --------------------------------------------------------------------
adduser_to_group(GroupName, UserName) ->
    case acldb:select_all_from_grouprecord(GroupName) of
	[] ->
	    {error, no_this_group};
	[Object] ->
	    {_, _GroupName1, GroupMemberList} = Object,
	    case lists:member(UserName, GroupMemberList) of
            true ->
                {error, user_has_exist_in_this_group};
            false ->
                NewGroupMemberList = [UserName|GroupMemberList],
                acldb:delete_object_from_db(GroupName),
                acldb:add_grouprecord_item(GroupName, NewGroupMemberList),
                %ets:insert(Tabgroup, {GroupName, NewGroupMemberList}),
                ok
	    end
    end.

%% --------------------------------------------------------------------
%% Function: deluser_from__grouptab/2
%% Description: delete a user from group member list
%% Returns: {ok, NewGroupMemberList}             |
%%          {ok, no_this_group}                  |
%%          {ok, user_not_exist_in_this_group}}  |
%% --------------------------------------------------------------------
deluser_from_group(GroupName, UserName) ->
    case acldb:select_all_from_grouprecord(GroupName) of
	[] ->
	    {error, no_this_group};
	[Object] ->
	    {_, _GroupName1, GroupMemberList} = Object,
	    case lists:member(UserName, GroupMemberList) of
            true ->
                acldb:delete_object_from_db(GroupName),
                NewGroupMemberList = lists:delete(UserName, GroupMemberList),
                acldb:add_grouprecord_item(GroupName, NewGroupMemberList),
                ok;
            false ->
                {ok, user_not_exist_in_the_group}
	    end
    end.

lookup_grouptab(GroupName) ->
    case acldb:select_all_from_grouprecord(GroupName) of
        [] ->
            %io:format("[Acl:~p]:group not in grouptab:~p~n",[?LINE, GroupName]),
            false;
        [_GroupObject] ->
            true
    end.
    
lookup_grouptab(GroupName, UserName) ->
    case acldb:select_all_from_grouprecord(GroupName) of
        [] ->
            %io:format("[Acl:~p]:group not in grouptab:~p~n",[?LINE, GroupName]),
            false;
        [GroupObject] ->
            {grouprecord, _GroupNameTemp, GroupMembers} = GroupObject,
            %io:format("[Acl:~p]GroupObject:~p~n",[?LINE, GroupObject]),
            lists:member(UserName, GroupMembers)
    end.


ls_table(TableName) ->
    acldb:select_all_from_Table(TableName).

ls_groups() ->
    acldb:select_all_from_grouptab(grouprecord).

ls_groupmembers(GroupName) ->
    case acldb:select_all_from_grouprecord(GroupName) of
        [] ->
            false;
        [GroupObject] ->
            {grouprecord, _GroupNameTemp, GroupMembers} = GroupObject,
            %io:format("[Acl:~p]GroupObject:~p~n",[?LINE, GroupObject]),
            GroupMembers
    end.
