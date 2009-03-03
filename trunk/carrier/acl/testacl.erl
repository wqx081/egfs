%%%-------------------------------------------------------------------
%%% File    : grouptab.erl
%%% Author  : 
%%% Description : the access control list functions
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(testacl).
%-include("../include/egfs.hrl").
%-export([set_grouptab/0]).
-compile(export_all).

start() ->
    acl:start().

reset() ->
    acldb:reset_tables().

opt() ->
    Return1 = acl:opt(read, "/public/hxm.txt", hxm),
    io:format("[Acl:~p]~p~n",[?LINE, Return1]),
    Return2 = acl:opt(write, "/public/hxm.txt", hxm),
    io:format("[Acl:~p]~p~n",[?LINE, Return2]),
    Return3 = acl:opt(delete, "/public/hxm.txt", hxm),
    io:format("[Acl:~p]~p~n",[?LINE, Return3]),

    Return4 = acl:opt(read, "/private/hxm.txt", hxm),
    io:format("[Acl:~p]~p~n",[?LINE, Return4]),
    Return5 = acl:opt(write, "/private/hxm.txt", hxm),
    io:format("[Acl:~p]~p~n",[?LINE, Return5]),
    Return6 = acl:opt(delete, "/private/hxm.txt", hxm),
    io:format("[Acl:~p]~p~n",[?LINE, Return6]),
    Return8 = acl:opt(read, "/private/hxm.txt", xcc),
    io:format("[Acl:~p]~p~n",[?LINE, Return8]),
    Return9 = acl:opt(write, "/private/hxm.txt", xcc),
    io:format("[Acl:~p]~p~n",[?LINE, Return9]),
    Return10 = acl:opt(delete, "/private/hxm.txt", xcc),
    io:format("[Acl:~p]~p~n",[?LINE, Return10]),
    Return7 = acl:opt(read, "/any/hxm.txt", hxm),
    io:format("[Acl:~p]~p~n",[?LINE, Return7]).
    
setacl()->
    Return1 = acl:setacl("/a/b/c/xuchuncong.txt", llk, user, 6),
    io:format("[Acl:~p]~p~n",[?LINE, Return1]),
    Return2 = acl:setacl("/a/b/c/xuchuncong.txt", group2, group, 4),
    io:format("[Acl:~p]~p~n",[?LINE, Return2]),
    Return3 = acl:setacl("/a/b/c/xuchuncong.txt", xcc, group, 6),
    io:format("[Acl:~p]~p~n",[?LINE, Return3]),
    Return4 = acl:setacl("/a/b", user1, group, 6),
    io:format("[Acl:~p]~p~n",[?LINE, Return4]),
    
    Return5 = acl:setacl("/private/hxm.txt", xcc, user, 7),
    io:format("[Acl:~p]~p~n",[?LINE, Return5]),
    Return6 = acl:setacl("/private/hxm.txt", xcc, group, 6),
    io:format("[Acl:~p]~p~n",[?LINE, Return6]).
    
delete_aclrecord(FileorFolderName) ->
    acl:delete_aclrecord(FileorFolderName).

%% --------------------------------------------------------------------
%% Function: set_grouptab/0
%% Description: new a grouptab and creat groups(user1\user2\user3)
%% Returns:tid()                           |
%% --------------------------------------------------------------------
set_grouptab() ->
    grouptab:addgroup_to_grouptab(group1),
    grouptab:addgroup_to_grouptab(group2),
    grouptab:addgroup_to_grouptab(group3),
    grouptab:adduser_to_group(group1, hxm),
    grouptab:adduser_to_group(group1, llk),
    grouptab:adduser_to_group(group1, xpz),
    grouptab:adduser_to_group(group2, xcc),
    grouptab:adduser_to_group(group2, lsb),
    grouptab:adduser_to_group(group2, pyy),
    grouptab:adduser_to_group(group3, zyb),
    grouptab:adduser_to_group(group3, njr),
    grouptab:adduser_to_group(group3, zm).


set_mnesia(Number, _FileName) when Number =:= 0 ->
    ok;
set_mnesia(Number, FileName) ->
    NumberT = Number -1,
    %io:format("~p~n",[NumberT]),
    acldb:add_aclrecord_item(FileName, [{root, 7}], [], []),
    FileNameT = string:concat(FileName, "/a"),
    set_mnesia(NumberT, FileNameT).

prefix() ->
    %io:format("[Acl:~p]time:~p~n",[?LINE, erlang:now()]),
    {_Before1, Before2, Before3} = erlang:now(),
    acldb:select_all_from_aclrecord_prefix_is("/a"),
    %io:format("[Acl:~p]time:~p~n",[?LINE, erlang:now()]).
    {_After1, After2, After3} = erlang:now(),
    DoneTime = (After2 - Before2) * 1000000 + After3 - Before3,
    io:format("    ~p~n",[DoneTime]).

testprefix(Number) when Number =:=4000 ->
    ok;
testprefix(Number) ->
    acldb:clear_tables(),
    %io:format("~p       ",[Number]),
    Wait_time = Number * 1000,
    mnesia:wait_for_tables([aclrecord, grouprecord, acllog], Wait_time),
    set_mnesia(Number, ""),
    mnesia:wait_for_tables([aclrecord, grouprecord, acllog], Wait_time),
    prefix(),
    testprefix(Number + 100).