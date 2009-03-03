%%%-------------------------------------------------------------------
%%% File    : acllib.erl
%%% Author  : 
%%% Description : the access control list functions
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(lib_acl).
%-behaviour(gen_server).
-include("../include/egfs.hrl").
%%gen_server callbacks
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
lookup_acltab(Optype, FileName, UserName, Type) ->
    case get_CtrlValue(FileName, UserName, Type) of
        {ok, CtrlValue, _Object} ->
            understand_ctrlvalue(Optype, CtrlValue);
        {error, file_not_exist} ->
            false;
        {error, user_not_exist} ->
            false
    end.

get_CtrlValue(FileName, _UserName, _Type) when FileName =:= []->
    {error, file_not_exist};
get_CtrlValue(FileName, UserName, Type) ->
    case acldb:select_all_from_aclrecord(FileName) of
	[] ->
	    %io:format("[Acl:~p]:FileName is:~p~n",[?LINE, FileName]),
	    Father_folder = get_father_folder(FileName),
	    %io:format("[Acl:~p]:Father_folder:~p~n",[?LINE, Father_folder]),
	    get_CtrlValue(Father_folder, UserName, Type);
	[Object] ->
	    %%to look up if the user can operate this file
	    %io:format("[Acl:~p]:FileName is:~p~n",[?LINE, FileName]),
	    lookup_aclrecord(Object, UserName, Type)
    end.

get_father_folder(FileName) ->
    %StringLen = string:len(FileName),
    Location = string:rchr(FileName, 47),
    case Location of
        1 ->
            string:substr(FileName, 1, Location);
        _Any ->
            string:substr(FileName, 1, Location -1)
    end.

lookup_aclrecord(Object, UserName, Type) ->
    {aclrecord, _FileName, UserList, GroupList, OtherList} = Object,
    %% 1.to lookup userlist 2.to lookup grouplist 3.to lookup otherlist
    %io:format("[Acl:~p]:userName is:~p, UserList is:~p~n",[?LINE, UserName, UserList]),
    case Type of
        user ->
            case lists:keysearch(UserName, 1, UserList) of
                {value, Tuple} ->
                    {_UserNameTemp, CtrlValue_User} = Tuple,
                    {ok, CtrlValue_User, Object};
                false ->
                    case lookup_GroupList(UserName, GroupList, Type) of
                        {ok, CtrlValue_Group} ->
                            {ok, CtrlValue_Group, Object};
                        {error, user_not_exist_in_group} ->
                            %%look up otherlist and return
                            lookup_OtherList(UserName, OtherList, Object)
                    end
            end;
        group ->
            case lookup_GroupList(UserName, GroupList, Type) of
                {ok, CtrlValue_Group} ->
                    {ok, CtrlValue_Group, Object};
                {error, user_not_exist_in_group} ->
                    {ok, 0, Object}
            end
        end.

lookup_GroupList(_UserorGroupName, Tuple, _Type) when Tuple =:=[]->
    {error, user_not_exist_in_group};
lookup_GroupList(UserorGroupName, Tuple, Type) ->
    %io:format("[Acl:~p]Tuple :~p~n",[?LINE, Tuple]),
    [H|T] = Tuple,
    %io:format("[Acl:~p]H:~p, T :~p~n",[?LINE, H, T]),
    {TempGroupName, CtrlValue} = H,
    Return = case Type of
                user ->
                    grouptab:lookup_grouptab(UserorGroupName, TempGroupName);
                group ->
                    string:equal(UserorGroupName, TempGroupName)
            end,
    case Return of
        true ->
            io:format("[Acl:~p]:you found username in grouptab~n",[?LINE]),
            io:format("[Acl:~p]:groupName ~p,CtrlValue ~p~n",[?LINE, UserorGroupName, CtrlValue]),
            {ok, CtrlValue};
        false ->
            lookup_GroupList(UserorGroupName, T, Type)
    end.

lookup_OtherList(UserName, OtherList, Object) ->
    case lists:keysearch(UserName, 1, OtherList) of
        {value, Tuple} ->
            {_UserNameTemp, CtrlValue_Other} = Tuple,
            {ok, CtrlValue_Other, Object};
        false ->
            case lists:keysearch(other, 1, OtherList) of
                {value, Tuple} ->
                    {other, CtrlValue_Other} = Tuple,
                    {ok, CtrlValue_Other, Object};
                false ->
                    {error, user_not_exist}
            end
    end.

understand_ctrlvalue(Optype, CtrlValue) ->
   CtrlResult= case Optype of
                    read ->
                         CtrlValue div 2#100;
                    write ->
                        CtrlTemp_w = CtrlValue rem 2#100,
                        CtrlTemp_w div 2#10;
                    append ->
                        CtrlTemp_a = CtrlValue rem 2#100,
                        CtrlTemp_a div 2#10;
                    delete ->
                        CtrlValue rem 2#10;
                    getacl ->
                        2
                end,
    case CtrlResult of
        0 ->
            false;
        1 ->
            true;
        2 ->
            CtrlValue
    end.
%
%lookup_acltab_and_grouptab(getacl, FileName, GroupName) ->
%    case lookup_grouptab(GroupName) of
%        ok ->
%
%        {error, no_this_group} ->
%            false
%    end.

lookup_acltab_copyfile(SourceFile, TargetFolder, UserName) ->
    %%TODO:如果源文件包含子文件或子文件夹，责应该循坏判断所有子文件和子文件夹的读操作权限
    case lookup_acltab(read, SourceFile, UserName, user) of
        true ->
            lookup_acltab(write, TargetFolder, UserName, user);
        false ->
            false
    end.

lookup_acltab_copyfolder(SourceFolder, TargetFolder, UserName) ->
    case opt_folder(read, SourceFolder, UserName) of
        true ->
            lookup_acltab(write, TargetFolder, UserName, user);
        false ->
            false
    end.

%%判断对含有子文件或子文件夹进行操作是否可行
opt_folder(Optype, FolderName, UserName) ->
    io:format("[Acl:~p]:FolderName is:~p~n",[?LINE, FolderName]),
    %io:format("[Acl:~p]:FolderName is:~p~n",[?LINE, FolderName]),
    case acldb:select_all_from_aclrecord_prefix_is(FolderName) of
        [] ->
            %FolderName不是任何一条记录的filename的前缀，需判断当前目录的删除权限
            io:format("[Acl:~p]:FolderName is:~p~n",[?LINE, FolderName]),
            lookup_acltab(Optype, FolderName, UserName, user);
        Object ->
            io:format("[Acl:~p]:Object is:~p~n",[?LINE, Object]),
            lookup_allaclrecord(Optype, Object, FolderName, UserName)
    end.

lookup_allaclrecord(Optype, Object, FolderName, UserName) when Object =:= []->
    %子目录和子文件均允许删除后，还需判断当前目录的删除权限
    lookup_acltab(Optype, FolderName, UserName, user);
lookup_allaclrecord(Optype, Object, FolderName, UserName) ->
    [H|T] = Object,
    io:format("[Acl:~p]:H is:~p~n",[?LINE, H]),
    case lookup_aclrecord(H, UserName, user) of
        {ok, CtrlValue, _ObjectTemp} ->
            case understand_ctrlvalue(Optype, CtrlValue) of
                true ->
                    io:format("[Acl:~p]:ok,T is:~p~n",[?LINE, T]),
                    lookup_allaclrecord(Optype, T, FolderName, UserName);
                false ->
                    false
            end;
        {error, user_not_exist} ->
            false
    end.

delete_aclrecord_of_fileorfolder(FileorFolderName) ->
    case acldb:select_all_from_aclrecord_prefix_is(FileorFolderName) of
        [] ->
            true;
        Object ->
            io:format("[Acl:~p]:Object is:~p~n",[?LINE, Object]),
            delete_aclrecord(Object)
    end.

delete_aclrecord(Object) when Object =:= []->
    true;
delete_aclrecord(Object) ->
    [H|T] = Object,
    io:format("[Acl:~p]:H is:~p~n",[?LINE, H]),
    acldb:delete_object_from_db(H),
    delete_aclrecord(T).

add_acl(FileName, UserorGroupName, CtrlValue, Type) ->
    %io:format("[Acl:~p]:add member:~p~n",[?LINE, UserorGroupName]),
    case acldb:select_all_from_aclrecord(FileName) of
        [] ->
            %%TODO:get_CtrlValue CtrlValue_Get
            case get_CtrlValue(FileName, UserorGroupName, Type) of
                {ok, CtrlValue_Get, Object} ->
                    if
                        CtrlValue_Get =:= CtrlValue ->
                            true;
                        true ->
                            {aclrecord, _File_Name, UserList, GroupList, OtherList} = Object,
                            acldb:add_aclrecord_item(FileName, UserList, GroupList, OtherList),
                            add_acl(FileName, UserorGroupName, CtrlValue, Type)
                    end;
                {error, file_not_exist} ->
                    false;
                {error, user_not_exist} ->
                    acldb:add_aclrecord_item(FileName, [{root, 7}], [], []),
                    add_acl(FileName, UserorGroupName, CtrlValue, Type)
            end;
        [Object] ->
            addmember_ctrl(UserorGroupName, CtrlValue, Object, Type)
    end.


addmember_ctrl(UserName, CtrlValue, Object, user) ->
    {aclrecord, FileName, UserList, GroupList, OtherList} = Object,
    NewList = addmember_to_list(UserName, CtrlValue, UserList),
    acldb:add_aclrecord_item(FileName, NewList, GroupList, OtherList),
    %io:format("[Acl:~p]:NewUserList is:~p~n", [?LINE, NewList]),
    true;
addmember_ctrl(GroupName, CtrlValue, Object, group) ->
    {aclrecord, FileName, UserList, GroupList, OtherList} = Object,
    case acldb:select_all_from_grouprecord(GroupName) of
	[] ->
	    %io:format("[Acl:~p]:group not in grouptab:~p~n",[?LINE, GroupName]),
	    false;
	[_GroupObject] ->
	    NewList = addmember_to_list(GroupName, CtrlValue, GroupList),
        acldb:add_aclrecord_item(FileName, UserList, NewList, OtherList),
	    %io:format("[Acl:~p]:NewGroupList is:~p~n", [?LINE, NewList]),
        true
    end.

addmember_to_list(UserorGroupName, CtrlValue, List) ->
    %io:format("[Acl:~p]:add member:~p~n",[?LINE, UserorGroupName]),
    %%鏌ヨ鍒楄〃涓槸鍚︽湁UserName瀵瑰簲鐨勫厓缁勶紝鏈夊垯鏇挎崲锛屾棤鍒欐坊鍔?
    case lists:keymember(UserorGroupName, 1, List) of
        true ->
            ListTemp = lists:keydelete(UserorGroupName, 1, List);
            %io:format("[Acl:~p]:new list is:~p~n", [?LINE, ListTemp]);
        false ->
            ListTemp = List
    end,
    case CtrlValue of
        0 ->
            ListTemp;
        _Any ->
            lists:append(ListTemp, [{UserorGroupName, CtrlValue}])
    end.
