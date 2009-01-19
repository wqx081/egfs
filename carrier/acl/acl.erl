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
%-include("acllib.erl").
-export([start/0, stop/0, terminate/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).
-import(acllib,[lookup_acltab/5, addmember/6]).
-import(grouptab, [set_grouptab/0]).
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
    gen_server:start_link({local, acl}, acl, [], []).

stop() ->
    gen_server:call(acl, stop).

readfile(FileName, UserName) ->
    gen_server:call(acl, {read, FileName, UserName}).

writefile(FileName, UserName) ->
    gen_server:call(acl, {write, FileName, UserName}).
    
appendfile(FileName, UserName) ->
    gen_server:call(acl, {append, FileName, UserName}).

delfile(FileName, UserName) ->
    gen_server:call(acl, {del, FileName, UserName}).

setuseracl(FileName, UserName, CtrlValue) ->
    gen_server:call(acl, {setuseracl, FileName, UserName, CtrlValue}).

setgroupacl(FileName, GroupName, CtrlValue) ->
    gen_server:call(acl, {setgroupacl, FileName, GroupName, CtrlValue}).

init([]) ->
    Tabgroup = set_grouptab(),
    Tabacl = ets:new(acl, []),
    ets:insert(Tabacl, {"/", [{root, 7}], [{group, 3}], [{other, 1}]}),
    {ok, {Tabacl, Tabgroup}}.

%%----------------------------------------------------------------------
%% handle_call functions for acl information
%%----------------------------------------------------------------------
handle_call({setuseracl, FileName, UserName, CtrlValue}, {_From, _}, {Tabacl, Tabgroup}) ->
    Reply = addmember(Tabacl, Tabgroup, FileName, UserName, CtrlValue, user),
    {reply, Reply, {Tabacl, Tabgroup}};

handle_call({setgroupacl, FileName, GroupName, CtrlValue}, {_From, _}, {Tabacl, Tabgroup}) ->
    Reply = addmember(Tabacl, Tabgroup, FileName, GroupName, CtrlValue, group),
    {reply, Reply, {Tabacl, Tabgroup}};

handle_call({Optype, FileName, UserName}, _From, {Tabacl, Tabgroup}) ->
    Reply = lookup_acltab(Tabacl, Tabgroup, Optype, FileName, UserName),
    {reply, Reply, {Tabacl, Tabgroup}}.

handle_cast(_Msg, State)    -> {noreply, State}.
handle_info(_Info, State)   -> {noreply, State}.
terminate(_Reason, _State)  -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
