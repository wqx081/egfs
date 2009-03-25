-module(meta_server).
-behaviour(gen_server).

-include("../include/header.hrl").




-export([start/0,stop/0]).
-export([init/1,handle_call/3,handle_cast/2,handle_info/2,terminate/2, code_change/3]).
-export([open/2,writeAllocate/1,locateChunk/2,registerChunk/4,close/1]).
-compile(export_all).

init(_Arg) ->
    process_flag(trap_exit, true),
%%     io:format("meta server starting~n"),
    %init_mnesia(),
    {ok, []}.

start() ->
    meta_db:start_mnesia(),    
%%    {ok,Tref} = timer:apply_interval((?CHUNKMAPPING_BROADCAST_INTERVAL),hostMonitor,broadcast,[]), % check host health every 5 second
    
    gen_server:start_link(?META_SERVER, meta_server, [], []).

stop() ->
    gen_server:cast(?META_SERVER, stop).

terminate(_Reason, _State) ->
      io:format("meta server terminating.abc.~n").

%%% from name server.

%%"name server" methods
%% 1: open file
%% FilePathName->string().
%% Mode -> w|r|a
%% UserName -> <<integer():64>>
%% return -> {ok, FileID} | {error, []}
handle_call({open, FilePathName, Mode, _UserName}, {_From, _}, State) ->
%%     io:format("inside handle_call_open, FileName:~p,Mode:~p,Token:~p~n",[FilePathName,Mode,_UserName]),
    Reply = meta_common:do_open(FilePathName, Mode, _UserName),
    {reply, Reply, State};


%%"name server" methods
%% 2: delete file
%% FilePathName->string().
%% _UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({delete, FilePathName, _UserName}, {_From, _}, State) ->
%%     io:format("inside handle_call_delete, FilePathName:~p,_UserName:~p~n",[FilePathName,_UserName]),
    Reply = meta_common:do_delete(FilePathName, _UserName),
    {reply, Reply, State};


%%"name server" methods
%% 3: copy file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% _UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({copy, SrcFilePathName, DstFilePathName, _UserName}, {_From, _}, State) ->
%%     io:format("inside handle_call_copy, SrcFilePathName:~p, DstFilePathName:~p, _UserName:~p~n",[SrcFilePathName, DstFilePathName, _UserName]),
    Reply = meta_common:do_copy(SrcFilePathName, DstFilePathName, _UserName),
    {reply, Reply, State};




%%"name server" methods
%% 4: move file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% _UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({move, SrcFilePathName, DstFilePathName, _UserName}, {_From, _}, State) ->
%%     io:format("inside handle_call_move, SrcFilePathName:~p, DstFilePathName:~p, _UserName:~p~n",[SrcFilePathName, DstFilePathName, _UserName]),
    Reply = meta_common:do_move(SrcFilePathName, DstFilePathName, _UserName),
    {reply, Reply, State};


%%"name server" methods
%% 5: list file/directory
%% FilePathName->string().
%% _UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({list, FilePathName, _UserName}, {_From, _}, State) ->
%%     io:format("inside handle_call_list, FilePathName:~p,_UserName:~p~n",[FilePathName, _UserName]),
    Reply = meta_common:do_list(FilePathName, _UserName),
    {reply, Reply, State};
%%"name server" methods
%% 6: mkdir file/directory
%% PathName->string().
%% _UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}


handle_call({mkdir, PathName, _UserName}, {_From, _}, State) ->
%%     io:format("inside handle_call_mkdir, PathName:~p,_UserName:~p~n",[PathName, _UserName]),
    Reply = meta_common:do_mkdir(PathName, _UserName),
    {reply, Reply, State};



%%"name server" methods
%% 7: change mod
%% FileName->string().
%% _UserName->string().
%% UserType->user/group
%%CtrlACL->0~7
%% _UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({chmod, FileName, _UserName, UserType, CtrlACL}, {_From, _}, State) ->
%%     io:format("inside handle_call_chmod, FileName:~p~n _UserName:~p~n UserType:~p~n CtrlACL:~p~n",[FileName, _UserName, UserType, CtrlACL]),
    Reply = meta_common:do_chmod(FileName, _UserName, UserType, CtrlACL),
    {reply, Reply, State};



handle_call({registerchunk,ChunkID,Host},{_From, _}, State ) ->
%%     error_logger:info_msg("registerchunk~n"),
    Reply = meta_common:do_register_replica(ChunkID,Host),
    {reply,Reply,State};


handle_call({getorphanchunk, HostRegName},{_From,_},State) ->
%%     io:format("inside handle_call_getorphanchunk,HostRegName:~p~n",[HostRegName]),
	Reply = meta_common:do_collect_orphanchunk(HostRegName),
    {reply, Reply, State};

%% handle_call({getfileinfo,FileName,_UserName}, {_From, _}, State) -> 
%% %%  error_logger:info_msg("getfileinfo _ ,File: ~p, User: ~n",[FileName,_UserName]),
%%     
%%     Reply  = meta_db:select_all_from_filemeta_byName(FileName),
%% 	{reply, Reply, State};

handle_call({debug,Arg},{_From,_},State) ->
    Reply = meta_common:do_debug(Arg),
    {reply,Reply,State};

handle_call({showWorker,WorkerFileName,EasyMod},{_From,_},State) ->
    Reply = meta_common:do_showWorker(WorkerFileName,EasyMod),
    {reply,Reply,State};


handle_call(MSG, {_From, _}, State) ->
     error_logger:info_msg("inside handle_call_error, MSG: ~p~n",[MSG]),
	Reply = {error, "undefined handler"},
    {reply, Reply, State}.

handle_cast({bootreport,HostName, ChunkList},State) ->
%%     io:format("inside handle_call_bootreport,HostInfoRec:~p~n",[HostName]),
%% 	meta_common:do_dataserver_bootreport(HostInfoRec, ChunkList),
    meta_db:do_register_dataserver(HostName,ChunkList),
    {noreply,State};

handle_cast(stop, State) ->
%%     io:format("meta server stopping~n"),
    {stop, normal, State}.


handle_info({'EXIT', _Pid, Why}, State) ->
	{stop, Why, State};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%%%"client api" methods
% write operation sequence:
% 1. open(client) =>
% 2. writeAllocate(client) =>
% ****3. registerChunk(chunkserver) =>
% 4. close(client)

% read operation sequence:
% 1. open(client) =>
% 2. locateChunk(client) =>
% 3. close(client)

%% delete operation:
%% 1. delete(FileID)

open(FileName,Mode) ->
    gen_server:call(?META_SERVER, {open, FileName,Mode}).

open2(FileName,Mode) ->
    gen_server:call(?META_SERVER,{open2,FileName,Mode}).

writeAllocate(FileID) ->
    gen_server:call(?META_SERVER, {allocatechunk, FileID}).

locateChunk(FileID, ChunkIndex)->
    gen_server:call(?META_SERVER, {locatechunk, FileID, ChunkIndex}).

close(FileID)->
    gen_server:call(?META_SERVER, {close, FileID}).

delete(FileID)->
    gen_server:call(?META_SERVER, {delete, FileID}).

%% "chunk server api" methods
%% write operation
%% 3. registerChunk(chunkserver) =>

%% boot report methods
%% 1. bootReport(HostInfoRec, ChunkList)

registerChunk(FileID, ChunkID, ChunkUsedSize, NodeList)->
    gen_server:call(?META_SERVER, {registerchunk, FileID, ChunkID, ChunkUsedSize, NodeList}).

%%
%% bootReport(HostInfoRec, ChunkList) => {ok, OrphanChunkList} | {error, "Reason for error"}
bootReport(HostInfoRec, ChunkList)->
    gen_server:call(?META_SERVER, {bootreport, HostInfoRec, ChunkList}).

%%
%% garbage collection methods
%% get orphan chunks
%% 1. getOrphanChunk(HostRegName) => [<<OrphanChunkID:64>>, ...]
getOrphanChunk(HostProcName)->
    gen_server:call(?META_SERVER, {getorphanchunk, HostProcName}).

%%
%% get file attributes methods
%% 1. open file to get FileID
%% 2. getFileAttr(FileID) => {ok, [FileSize, <<CreateTime:64>>, <<ModifyTime:64>>, ACL]}
getFileAttr(FileID)->
    gen_server:call(?META_SERVER, {getfileattr, FileID}).


heartBeat(HostInfoRec) ->
    gen_server:call(?META_SERVER,{heartbeat,HostInfoRec}).

debug(Arg) ->
    gen_server:call(?META_SERVER,{debug,Arg}).







%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
