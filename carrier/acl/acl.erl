%%%-------------------------------------------------------------------
%%% File    : acl.erl
%%% Author  : 
%%% Description : the access control list functions
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(acl).
-behaviour(gen_server).
%-include("../include/egfs.hrl").
%-include("acllib.erl").
-export([start/0, stop/0, terminate/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).
%%gen_server callbacks
-compile(export_all).

%%----------------------------------------------------------------------
%% client_api,can be used for application progamming
%%----------------------------------------------------------------------
start() ->
    gen_server:start_link({local, acl}, acl, [], []).

stop() ->
    gen_server:call(acl, stop).

newfile(FileName, UserName) ->
    gen_server:call(acl, {new, FileName, UserName}).

readfile(FileName, UserName) ->
    gen_server:call(acl, {read, FileName, UserName}).

writefile(FileName, UserName) ->
    gen_server:call(acl, {write, FileName, UserName}).
    
appendfile(FileName, UserName) ->
    gen_server:call(acl, {append, FileName, UserName}).

delfile(FileName, UserName) ->
    gen_server:call(acl, {del, FileName, UserName}).

adduser(FileName, UserName, CtrlValue) ->
    gen_server:call(acl, {adduser, FileName, UserName, CtrlValue}).

addgroup(FileName, GroupName, CtrlValue) ->
    gen_server:call(acl, {addgroup, FileName, GroupName, CtrlValue}).

init([]) ->
    {ok, ets:new(acl, [])}.

%%----------------------------------------------------------------------
%% handle_call functions
%%----------------------------------------------------------------------
handle_call({new, FileName, UserName}, {_From, _}, Tabacl) ->
    Reply = case ets:lookup(Tabacl, FileName) of
	[] ->
	    UserList =  [{root, ?UserCtrl}, {UserName, ?UserCtrl}],
	    GroupList = [{group, ?GroupCtrl}],
	    OtherList = [{other, ?OtherCtrl}],
	    ets:insert(Tabacl, {FileName, UserList, GroupList, OtherList}),
	    io:format("[Acl:~p]FileName is:~p~n",[?LINE, FileName]),
	    {ok, {FileName, UserList, GroupList, OtherList}};
	[Object] ->
	    io:format("[Acl:~p]return is:~p~n",[?LINE, Object]),
	    {FileName, file_has_exist}
	    end,
    {reply, Reply, Tabacl};

handle_call({adduser, FileName, UserName, CtrlValue}, {_From, _}, Tabacl) ->
    Reply = addmember_to_List(Tabacl, FileName, UserName, CtrlValue, user),
    {reply, Reply, Tabacl};

handle_call({addgroup, FileName, GroupName, CtrlValue}, {_From, _}, Tabacl) ->
    Reply = addmember_to_List(Tabacl, FileName, GroupName, CtrlValue, group),
    {reply, Reply, Tabacl};

handle_call({Optype, FileName, UserName}, _From, Tabacl) ->
    Reply = lookup_acltab(Tabacl, Optype, FileName, UserName),
    {reply, Reply, Tabacl}.

handle_cast(_Msg, State)    -> {noreply, State}.
handle_info(_Info, State)   -> {noreply, State}.
terminate(_Reason, _State)  -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
