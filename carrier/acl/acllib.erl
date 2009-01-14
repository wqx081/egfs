%%%-------------------------------------------------------------------
%%% File    : acl.erl
%%% Author  : 
%%% Description : the access control list functions
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(acllib).
%-behaviour(gen_server).
%-include("../include/egfs.hrl").
%%gen_server callbacks
-compile(export_all).

%%default setting，以后应该改成找到该文件的目录，继承acl
-define(UserCtrl, 2#111).   %% rwd
-define(GroupCtrl, 2#011).   %% rw-
-define(OtherCtrl, 2#001).   %% r--


%%----------------------------------------------------------------------
%% loop up ets and return ok or not
%%----------------------------------------------------------------------
lookup_acltab(_Tabacl, _Optype, FileName, _UserName) when FileName =:= []->
    {ok, file_does_not_exit};
lookup_acltab(Tabacl, Optype, FileName, UserName) ->
    case ets:lookup(Tabacl, FileName) of
	[] ->
	    io:format("[Acl:~p]:filename not in acltab~n",[?LINE]),
	    FileName1 = lists:reverse(FileName),
	    Father_folder = get_father_folder(FileName1),
	    io:format("[Acl:~p]:Father_folder:~p~n",[?LINE, Father_folder]),
	    lookup_acltab(Tabacl, Optype, Father_folder, UserName);
	[Object] ->
	    %%to look up if the user can operate this file
	    lookup_aclrecord(Optype, Object, UserName)
    end.

get_father_folder(FileName) ->
    [H|T] = FileName,
    %io:format("[Acl:~p]:H:~p,FileName:~p~n",[?LINE, H, FileName]),
    case H of
	47 ->
	    %io:format("[Acl:~p]:~n",[?LINE]),
	    lists:reverse(T);
	_Any ->
	    get_father_folder(T)
    end.

lookup_aclrecord(Optype, Object, UserName) ->
    {_FileName, UserList, GroupList, OtherList} = Object,
    %% 1.to lookup userlist 2.to lookup grouplist 3.to lookup otherlist
    io:format("[Acl:~p]:userName is:~p, UserList is:~p~n",[?LINE, UserName, UserList]),
    case lists:keysearch(UserName, 1, UserList) of
	{value, Tuple} ->
	    {_UserNameTemp, CtrlValue_User} = Tuple,
	    understand_ctrlvalue(Optype, CtrlValue_User);
	false ->
	    case lookup_list(UserName, GroupList) of
		{ok, CtrlValue_Group} ->
		    understand_ctrlvalue(Optype, CtrlValue_Group);
		{no, not_exist} ->
		    %%look up otherlist and return
		    [{other, CtrlValue_Other}] = OtherList,
		    understand_ctrlvalue(Optype, CtrlValue_Other)
	    end
    end.

lookup_list(_UserName, Tuple) when Tuple =:=[]->
    {no, not_exist};
lookup_list(UserName, Tuple) ->
    %io:format("[Acl:~p]Tuple :~p~n",[?LINE, Tuple]),
    [H|T] = Tuple,
    io:format("[Acl:~p]H:~p, T :~p~n",[?LINE, H, T]),
    {TempGroupName, CtrlValue} = H,
    case lookup_grouptab(UserName, TempGroupName) of
	true ->
	    io:format("[Acl:~p]:you found username in grouptab~n",[?LINE]),
	    {ok, CtrlValue};
	false ->
	    lookup_list(UserName, T)
    end.

lookup_grouptab(UserName, GroupName) ->
    Tabgroup = ets:new(tabgroup, []),
    ets:insert(Tabgroup, {user1, [zyb, njr, zm]}),
    ets:insert(Tabgroup, {admin, [hxm, llk, xpz]}),
    ets:insert(Tabgroup, {user2, [xcc, lsb, pyy]}),
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

addmember_to_List(Tabacl, FileName, AddName, CtrlValue, Type) ->
    case ets:lookup(Tabacl, FileName) of
	[] ->
	    {ok, no_this_file};
	[Object] ->
	    {FileName, UserList, GroupList, OtherList} = Object,
	    %%需查询列表中是否有UserName对应的元组，有则替换，没有则添加
	    case Type of
		user ->
		    NewList = lists:append(UserList, [{AddName, CtrlValue}]),
		    ets:delete_object(Tabacl, Object),
		    ets:insert(Tabacl, {FileName, NewList, GroupList, OtherList}),
		    io:format("[Acl:~p]:add NewList:~p~n", [?LINE, NewList]);
		group ->
		    NewList = lists:append(GroupList, [{AddName, CtrlValue}]),
		    ets:delete_object(Tabacl, Object),
		    ets:insert(Tabacl, {FileName, UserList, NewList, OtherList}),
		    io:format("[Acl:~p]:add NewList:~p~n", [?LINE, NewList]);
		Any ->
	            io:format("[Acl:~p]:don't understand type:~p~n", [?LINE, Any])
	    end,
	    {ok, addmember_ok}
    end.
