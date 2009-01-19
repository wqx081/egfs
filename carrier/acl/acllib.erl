%%%-------------------------------------------------------------------
%%% File    : acllib.erl
%%% Author  : 
%%% Description : the access control list functions
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(acllib).
%-behaviour(gen_server).
%-include("../include/egfs.hrl").
%%gen_server callbacks
-export([lookup_acltab/5, addmember/6]).
-compile(export_all).

%%----------------------------------------------------------------------
%% loop up ets and return ok or not
%%----------------------------------------------------------------------

%% --------------------------------------------------------------------
%% Function: open/2
%% Description: call metaserver to open file
%% Returns: {ok, Filedevice}          |
%%          {error, Why}              |
%% --------------------------------------------------------------------
lookup_acltab(Tabacl, Tabgroup, Optype, FileName, UserName) ->
    case get_CtrlValue(Tabacl, Tabgroup, FileName, UserName) of
	{ok, CtrlValue} ->
	    understand_ctrlvalue(Optype, CtrlValue);
	{no, not_exit} ->
	    {ok, no_exit}
    end.

get_CtrlValue(_Tabacl, _Tabgroup, FileName, _UserName) when FileName =:= []->
    {no, not_exit};
get_CtrlValue(Tabacl, Tabgroup, FileName, UserName) ->
    case ets:lookup(Tabacl, FileName) of
	[] ->
	    io:format("[Acl:~p]:FileName is:~p~n",[?LINE, FileName]),
	    FileName1 = lists:reverse(FileName),
	    [H|T] = FileName1,
	    case H of
		47 ->
		    FileNameTemp = T;
		_Any ->
		    FileNameTemp = FileName1
	    end,
	    Father_folder = get_father_folder(FileNameTemp),
	    io:format("[Acl:~p]:Father_folder:~p~n",[?LINE, Father_folder]),
	    get_CtrlValue(Tabacl, Tabgroup, Father_folder, UserName);
	[Object] ->
	    %%to look up if the user can operate this file
	    io:format("[Acl:~p]:FileName is:~p~n",[?LINE, FileName]),
	    lookup_aclrecord(Tabgroup, Object, UserName)
    end.

get_father_folder(FileName) ->
    [H|T] = FileName,
    %io:format("[Acl:~p]:H:~p,FileName:~p~n",[?LINE, H, FileName]),
    case H of
	47 ->
	    io:format("[Acl:~p]:T is:~p~n",[?LINE, T]),
	    lists:reverse(FileName);
	_Any ->
	    get_father_folder(T)
    end.

lookup_aclrecord(Tabgroup, Object, UserName) ->
    {_FileName, UserList, GroupList, OtherList} = Object,
    %% 1.to lookup userlist 2.to lookup grouplist 3.to lookup otherlist
    io:format("[Acl:~p]:userName is:~p, UserList is:~p~n",[?LINE, UserName, UserList]),
    case lists:keysearch(UserName, 1, UserList) of
	{value, Tuple} ->
	    {_UserNameTemp, CtrlValue_User} = Tuple,
	    {ok, CtrlValue_User};
	false ->
	    case lookup_list(Tabgroup, UserName, GroupList) of
		{ok, CtrlValue_Group} ->
		    {ok, CtrlValue_Group};
		{no, not_exist} ->
		    %%look up otherlist and return
		    [{other, CtrlValue_Other}] = OtherList,
		    {ok, CtrlValue_Other}
	    end
    end.

lookup_list(_Tabgroup, _UserName, Tuple) when Tuple =:=[]->
    {no, not_exist};
lookup_list(Tabgroup, UserName, Tuple) ->
    %io:format("[Acl:~p]Tuple :~p~n",[?LINE, Tuple]),
    [H|T] = Tuple,
    io:format("[Acl:~p]H:~p, T :~p~n",[?LINE, H, T]),
    {TempGroupName, CtrlValue} = H,
    case lookup_grouptab(Tabgroup, UserName, TempGroupName) of
	true ->
	    io:format("[Acl:~p]:you found username in grouptab~n",[?LINE]),
	    {ok, CtrlValue};
	false ->
	    lookup_list(Tabgroup, UserName, T)
    end.

lookup_grouptab(Tabgroup, UserName, GroupName) ->
    case ets:lookup(Tabgroup, GroupName) of
	[] ->
	    io:format("[Acl:~p]:group not in grouptab:~p~n",[?LINE, GroupName]),
	    false;
	[GroupObject] ->
	    {_GroupNameTemp, GroupMembers} = GroupObject,
	    io:format("[Acl:~p]GroupObject:~p~n",[?LINE, GroupObject]),
	    lists:member(UserName, GroupMembers)
    end.

understand_ctrlvalue(Optype, CtrlValue) ->
    case Optype of
	read ->
	    CtrlTemp_read = CtrlValue rem 2#10,
	    pass_or_reject(CtrlTemp_read);
	write ->
	    CtrlTemp_w = CtrlValue rem 2#100,
	    CtrlTemp_write = CtrlTemp_w div 2#10,
	    pass_or_reject(CtrlTemp_write);
	append ->
	    CtrlTemp_a = CtrlValue rem 2#100,
	    CtrlTemp_append = CtrlTemp_a div 2#10,
	    pass_or_reject(CtrlTemp_append);
	del ->
	    CtrlTemp_del = CtrlValue div 2#100,
	    pass_or_reject(CtrlTemp_del);
	Any ->
	    io:format("[Acl:~p]any is:~p~n",[?LINE, Any]),
	    {error, not_define_this_operation}
    end.

pass_or_reject(CtrlTemp) ->
    if
	CtrlTemp =:= 1 ->
	    {ok, pass};
	true ->
	    {ok, reject}
    end.

addmember(Tabacl, Tabgroup, FileName, AddName, CtrlValue, Type) ->
    io:format("[Acl:~p]:add member:~p~n",[?LINE, AddName]),
    case ets:lookup(Tabacl, FileName) of
	[] ->
	    %%判断父目录是否有CtrlValue这一权限，如果有就添加记录
	    case get_CtrlValue(Tabacl, Tabgroup, FileName, AddName) of
		{ok, CtrlValue_Get} ->
		    if
			CtrlValue_Get > CtrlValue ->
			    {ok, not_need_add};
			true ->
			    ets:insert(Tabacl, {FileName, [{root, 7}], [{group, 3}], [{other, 1}]}),
			    addmember(Tabacl, Tabgroup, FileName, AddName, CtrlValue, Type)
		    end;
		{no, not_exit} ->
		    ets:insert(Tabacl, {FileName, [{root, 7}], [{group, 3}], [{other, 1}]}),
		    addmember(Tabacl, Tabgroup, FileName, AddName, CtrlValue, Type)
	    end;
	[Object] ->
	    addmember_ctrl(Tabacl, Tabgroup, AddName, CtrlValue, Object, Type)
    end.

addmember_ctrl(Tabacl, Tabgroup, AddName, CtrlValue, Object, Type) ->
    case Type of
	user ->
	    adduser_to_userlist(Tabacl, AddName, CtrlValue, Object);
	group ->
	    addgroup_to_grouplist(Tabacl, Tabgroup, AddName, CtrlValue, Object);
	Any ->
            io:format("[Acl:~p]:you can only add user/group:~p~n", [?LINE, Any])
    end,
    {ok, addmember_ok}.

adduser_to_userlist(Tabacl, AddName, CtrlValue, Object) ->
    {FileName, UserList, GroupList, OtherList} = Object,
    NewList = addmember_to_list(AddName, CtrlValue, UserList),
    %ets:delete_object(Tabacl, Object),
    ets:insert(Tabacl, {FileName, NewList, GroupList, OtherList}),
    io:format("[Acl:~p]:NewUserList is:~p~n", [?LINE, NewList]).


addgroup_to_grouplist(Tabacl, Tabgroup, AddName, CtrlValue, Object) ->
    %%查询这个组在grouptab中是否存在，有则添加,无则返回false
    {FileName, UserList, GroupList, OtherList} = Object,
    case ets:lookup(Tabgroup, AddName) of
	[] ->
	    io:format("[Acl:~p]:group not in grouptab:~p~n",[?LINE, AddName]),
	    false;
	[_GroupObject] ->
	    NewList = addmember_to_list(AddName, CtrlValue, GroupList),
	    %ets:delete_object(Tabacl, Object),
	    ets:insert(Tabacl, {FileName, UserList, NewList, OtherList}),
	    io:format("[Acl:~p]:NewGroupList is:~p~n", [?LINE, NewList])
    end.

addmember_to_list(AddName, CtrlValue, List) ->
    io:format("[Acl:~p]:add member:~p~n",[?LINE, AddName]),
    %%查询列表中是否有UserName对应的元组，有则替换，无则添加
    case lists:keymember(AddName, 1, List) of
	true ->
	    ListTemp = lists:keydelete(AddName, 1, List),
	    io:format("[Acl:~p]:new list is:~p~n", [?LINE, ListTemp]);
	false ->
	    ListTemp = List
    end,
    lists:append(ListTemp, [{AddName, CtrlValue}]).
