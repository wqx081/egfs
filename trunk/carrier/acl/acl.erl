%%%-------------------------------------------------------------------
%%% File    : acl.erl
%%% Author  : 
%%% Description : the access control list functions
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(acl).
-behaviour(gen_server).
-include("../include/egfs.hrl").
%-include("acldb.erl").
-export([start/0, stop/0, terminate/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

%-import(acldb, [add_aclrecord_item/4]).
%%gen_server callbacks
-compile(export_all).

%% --------------------------------------------------------------------
%% Function: start/0, stop/0, readfile/2, writefile/2, appendfile/2
%%           delfile/2, setuseracl/3, setgroupacl/3 
%% Description: start acl server and send infomation to genserver for test
%% Returns:                    |
%%                             |
%% --------------------------------------------------------------------
start() ->
    gen_server:start_link(?ACL_SERVER, ?MODULE, [], []).

stop() ->
    gen_server:call(?ACL_SERVER, stop).

%%----------------------------------------------------------------------
%% to ask if the read/write/append/delete operation can do
%%----------------------------------------------------------------------
opt(read, FileName, UserName) ->
    io:format("[Acl:~p]:the ~p will read ~p~n",[?LINE, UserName, FileName]),
    gen_server:call(?ACL_SERVER, {read, FileName, UserName});
opt(write, FileName, UserName) ->
    io:format("[Acl:~p]:the ~p write read ~p~n",[?LINE, UserName, FileName]),
    gen_server:call(?ACL_SERVER, {write, FileName, UserName});
opt(append, FileName, UserName) ->
    io:format("[Acl:~p]:the ~p append read ~p~n",[?LINE, UserName, FileName]),
    gen_server:call(?ACL_SERVER, {append, FileName, UserName});
opt(delete, FileName, UserName) ->
    io:format("[Acl:~p]:the ~p delete read ~p~n",[?LINE, UserName, FileName]),
    gen_server:call(?ACL_SERVER, {delete, FileName, UserName}).

%%----------------------------------------------------------------------
%% to ask if the copy file operation can do
%%----------------------------------------------------------------------
copyfile(SourceFile, TargetFolder, UserName) ->
    gen_server:call(?ACL_SERVER, {copyfile, SourceFile, TargetFolder, UserName}).

%%----------------------------------------------------------------------
%% to ask if the copy folder operation can do
%%----------------------------------------------------------------------
copyfolder(SourceFolder, TargetFolder, UserName) ->
    gen_server:call(?ACL_SERVER, {copyfolder, SourceFolder, TargetFolder, UserName}).

%%----------------------------------------------------------------------
%% to ask if the delete_folder operation can do
%%----------------------------------------------------------------------
delete_folder(FolderName, UserName) ->
    gen_server:call(?ACL_SERVER, {delete_folder, FolderName, UserName}).

%%----------------------------------------------------------------------
%% to del acl_record after delete a file|folder ok
%%----------------------------------------------------------------------
delete_aclrecord(FileorFolderName) ->
    gen_server:call(?ACL_SERVER, {delete_aclrecord, FileorFolderName}).

%%----------------------------------------------------------------------
%% to set a file/folder acl
%%----------------------------------------------------------------------
setacl(FileName, UserName, Tpye, CtrlACL) ->
    gen_server:call(?ACL_SERVER, {setacl, FileName, UserName, Tpye, CtrlACL}).

%%----------------------------------------------------------------------
%% to get acl value of file/folder
%%----------------------------------------------------------------------
getacl(FileName, UserName, user) ->
    gen_server:call(?ACL_SERVER, {getacl, FileName, UserName, user});
getacl(FileName, GroupName, group) ->
    gen_server:call(?ACL_SERVER, {getacl, FileName, GroupName, group}).

%%----------------------------------------------------------------------
%% to init the acl server
%%----------------------------------------------------------------------
init([]) ->
    acldb:start_mnesia(),
    {ok, {}}.

%%----------------------------------------------------------------------
%% handle_call functions for acl information
%%----------------------------------------------------------------------
handle_call({Optype, FileName, UserName}, _From, {}) ->
    Reply = case Optype of
                delete_folder ->
                    lib_acl:opt_folder(Optype, FileName, UserName);
                _Any ->
                    lib_acl:lookup_acltab(Optype, FileName, UserName, user)
            end,
    {reply, Reply, {}};

handle_call({copyfile, SourceFile, TargetFolder, UserName}, _From, {}) ->
    Reply = lib_acl:lookup_acltab_copyfile(SourceFile, TargetFolder, UserName),
    {reply, Reply, {}};

handle_call({copyfolder, SourceFolder, TargetFolder, UserName}, _From, {}) ->
    Reply = lib_acl:lookup_acltab_copyfolder(SourceFolder, TargetFolder, UserName),
    {reply, Reply, {}};

handle_call({delete_aclrecord, FileorFolderName}, _From, {}) ->
    Reply = lib_acl:delete_aclrecord_of_fileorfolder(FileorFolderName),
    {reply, Reply, {}};

handle_call({setacl, FileName, UserName, Type, CtrlACL}, {_From, _}, {}) ->
    io:format("[Acl:~p]:~p will set ~p, acl value is:~p~n",[?LINE, UserName, FileName, CtrlACL]),
    Reply = case Type of
                user ->
                    lib_acl:add_acl(FileName, UserName, CtrlACL, user);
                group ->
                    lib_acl:add_acl(FileName, UserName, CtrlACL, group);
                _Any ->
                    io:format("haven't define this type of setacl~n"),
                    {error, "no this type"}
            end,
    {reply, Reply, {}};

handle_call({getacl, FileName, UserName, Type}, {_From, _}, {}) ->
    Reply = case Type of
                user ->
                    lib_acl:lookup_acltab(getacl, FileName, UserName, user);
                group ->
                    lib_acl:lookup_acltab(getacl, FileName, UserName, group)
            end,
    {reply, Reply, {}}.

handle_cast(_Msg, State)    -> {noreply, State}.
handle_info(_Info, State)   -> {noreply, State}.
terminate(_Reason, _State)  -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.



