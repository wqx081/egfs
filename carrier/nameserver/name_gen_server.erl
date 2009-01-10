-module(name_gen_server).
-behaviour(gen_server).

-include("./include/egfs.hrl").
-import(name_server, [do_open/3, do_delete/2, do_copy/3, do_move/3, do_list/2, do_mkdir/2]).

-export([start/0,stop/0,terminate/2]).
-export([init/1, handle_call/3, handle_cast/2,handle_info/2]).


-define(NAME_SERVER, {global, name_server}).
%-import(metaserver, [do_open/3]).


-compile(export_all).

%-define(GM,{global, metagenserver}).

init(_Arg) ->
    process_flag(trap_exit, true),
    io:format("name server starting~n"),
    %init_mnesia(),
    {ok, []}.

start() ->
%    {ok,TrefMonitor} = timer:apply_interval((?HEART_BEAT_TIMEOUT),hostMonitor,checkHostHealth,[]), % check host health every 5 second
%    {ok,TrefCollect} = timer:apply_interval((?GARBAGE_COLLECT_PERIOD),metaDB,do_find_orphanchunk,[]),
    gen_server:start_link(?NAME_SERVER, name_gen_server, [], []).

stop() ->
    gen_server:cast(?NAME_SERVER, stop).

terminate(_Reason, _State) ->
    io:format("name server terminating~n").


%%"name server" methods
%% 1: open file
%% FilePathName->string().
%% Mode -> w|r|a
%% UserToken -> <<integer():64>>
%% return -> {ok, FileID} | {error, []}
handle_call({open, FilePathName, Mode, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_open, FileName:~p,Mode:~p,Token:~p~n",[FilePathName,Mode,UserToken]),
    Reply = do_open(FilePathName, Mode, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 2: delete file/directory
%% FilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({delete, FilePathName, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_delete, FilePathName:~p,UserToken:~p~n",[FilePathName,UserToken]),
    Reply = do_delete(FilePathName, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 3: copy file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({copy, SrcFilePathName, DstFilePathName, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_copy, SrcFilePathName:~p, DstFilePathName:~p, UserToken:~p~n",[SrcFilePathName, DstFilePathName, UserToken]),
    Reply = do_copy(SrcFilePathName, DstFilePathName, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 4: move file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({move, SrcFilePathName, DstFilePathName, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_move, SrcFilePathName:~p, DstFilePathName:~p, UserToken:~p~n",[SrcFilePathName, DstFilePathName, UserToken]),
    Reply = do_move(SrcFilePathName, DstFilePathName, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 5: list file/directory
%% FilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({list, FilePathName, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_list, FilePathName:~p,UserToken:~p~n",[FilePathName, UserToken]),
    Reply = do_list(FilePathName, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 6: mkdir file/directory
%% PathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({mkdir, PathName, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_mkdir, PathName:~p,UserToken:~p~n",[PathName, UserToken]),
    Reply = do_mkdir(PathName, UserToken),
    {reply, Reply, State};

%% --------------------------------------------------------------------
%% Function:
%% Arg     :		NodeList must be a list
%% Description:
%% Returns:
%% --------------------------------------------------------------------
handle_call(_, {_From, _}, State)->
    io:format("inside handle_call_error~n"),
	Reply = {error, "undefined handler"},
    {reply, Reply, State}.


handle_cast(stop, State) ->
    io:format("name server stopping~n"),
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% delete operation:
%% 1. delete(FileID)

open(FilePathName, Mode, UserToken) ->
    gen_server:call(?NAME_SERVER, {open, FilePathName, Mode, UserToken}).

delete(FilePathName, UserToken)->
    gen_server:call(?NAME_SERVER, {delete, FilePathName, UserToken}).

copy(SrcFilePathName, DstFilePathName, UserToken)->
    gen_server:call(?NAME_SERVER, {copy, SrcFilePathName, DstFilePathName, UserToken}).

move(SrcFilePathName, DstFilePathName, UserToken)->
    gen_server:call(?NAME_SERVER, {move, SrcFilePathName, DstFilePathName, UserToken}).

list(FilePathName, UserToken)->
    gen_server:call(?NAME_SERVER, {list, FilePathName, UserToken}).

mkdir(PathName, UserToken)->
    gen_server:call(?NAME_SERVER, {list, PathName, UserToken}).