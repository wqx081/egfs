-module(meta_server).
-behaviour(gen_server).

-include("../include/header.hrl").




-export([start/0,stop/0]).
-export([init/1,handle_call/3,handle_cast/2,handle_cast/3,handle_info/2,terminate/2, code_change/3]).
-export([open/2,writeAllocate/1,locateChunk/2,registerChunk/4,close/1]).
-compile(export_all).

init(_Arg) ->
    process_flag(trap_exit, true),
    io:format("meta server starting~n"),
    %init_mnesia(),
    {ok, []}.

start() ->
    meta_db:start_mnesia(),
    {ok,_Tref} = timer:apply_interval((?NODE_CHECK_INTERVAL),hostMonitor,checkNodes,[]), % check host health every 5 second
 %%   {ok,Tref} = timer:apply_interval((?CHUNKMAPPING_BROADCAST_INTERVAL),hostMonitor,broadcast,[]), % check host health every 5 second
    
    gen_server:start_link(?META_SERVER, meta_server, [], []).

stop() ->
    gen_server:cast(?META_SERVER, stop).

terminate(_Reason, _State) ->
    io:format("meta server terminating~n").




%%% from name server.

%%"name server" methods
%% 1: open file
%% FilePathName->string().
%% Mode -> w|r|a
%% UserName -> <<integer():64>>
%% return -> {ok, FileID} | {error, []}
handle_call({open, FilePathName, Mode, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_open, FileName:~p,Mode:~p,Token:~p~n",[FilePathName,Mode,UserName]),
    Reply = meta_common:do_open(FilePathName, Mode, UserName),
    {reply, Reply, State};



%%"name server" methods
%% 2: delete file
%% FilePathName->string().
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({delete, FilePathName, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_delete, FilePathName:~p,UserName:~p~n",[FilePathName,UserName]),
    Reply = meta_common:do_delete(FilePathName, UserName),
    {reply, Reply, State};




%%"name server" methods
%% 3: copy file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({copy, SrcFilePathName, DstFilePathName, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_copy, SrcFilePathName:~p, DstFilePathName:~p, UserName:~p~n",[SrcFilePathName, DstFilePathName, UserName]),
    Reply = meta_common:do_copy(SrcFilePathName, DstFilePathName, UserName),
    {reply, Reply, State};




%%"name server" methods
%% 4: move file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({move, SrcFilePathName, DstFilePathName, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_move, SrcFilePathName:~p, DstFilePathName:~p, UserName:~p~n",[SrcFilePathName, DstFilePathName, UserName]),
    Reply = meta_common:do_move(SrcFilePathName, DstFilePathName, UserName),
    {reply, Reply, State};





%%"name server" methods
%% 5: list file/directory
%% FilePathName->string().
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({list, FilePathName, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_list, FilePathName:~p,UserName:~p~n",[FilePathName, UserName]),
    Reply = meta_common:do_list(FilePathName, UserName),
    {reply, Reply, State};
%%"name server" methods
%% 6: mkdir file/directory
%% PathName->string().
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}


handle_call({mkdir, PathName, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_mkdir, PathName:~p,UserName:~p~n",[PathName, UserName]),
    Reply = meta_common:do_mkdir(PathName, UserName),
    {reply, Reply, State};



%%"name server" methods
%% 7: change mod
%% FileName->string().
%% UserName->string().
%% UserType->user/group
%%CtrlACL->0~7
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({chmod, FileName, UserName, UserType, CtrlACL}, {_From, _}, State) ->
    io:format("inside handle_call_chmod, FileName:~p~n UserName:~p~n UserType:~p~n CtrlACL:~p~n",[FileName, UserName, UserType, CtrlACL]),
    Reply = meta_common:do_chmod(FileName, UserName, UserType, CtrlACL),
    {reply, Reply, State};
%% function moved to meta_worker.------------------->

%% handle_call({allocatechunk, FileID}, {From, _}, State) ->
%%     io:format("inside handle_call_allocatechunk, FileID:~p,From:~p~n",[FileID,From]),
%%     Reply = do_allocate_chunk(FileID, From),
%%     {reply, Reply, State};

%% handle_call({registerchunk, FileID, ChunkID, ChunkUsedSize, NodeList},
%%             _From, State) ->    
%%     io:format("~p",[time()]),
%%     io:format("inside handle_call_registerchunk,FileID:~p,ChunkID:~p,ChunkUsedSize:~p~n",[FileID,ChunkID,ChunkUsedSize]),
%%     io:format("NodeList~p~n",[list_to_tuple(NodeList)]),
%%     Reply = do_register_chunk(FileID, ChunkID, ChunkUsedSize, NodeList),
%%     {reply, Reply, State};

%% handle_call({locatechunk, FileID, ChunkIndex}, _From, State) ->
%%     io:format("inside handle_call_locatechunk,FileID:~p,ChunkIndex:~p~n",[FileID,ChunkIndex]),
%%     Reply = do_get_chunk(FileID, ChunkIndex),
%%     {reply, Reply, State};

%% handle_call({close, FileID}, {From, _}, State)->
%%     io:format("inside handle_call_close,FileID:~p,From:~p~n",[FileID,From]),
%% 	Reply = do_close(FileID, From),
%%     {reply, Reply, State};
%% <-------------------    function moved to meta_worker.

%% handle_call({delete,FileName},{From,_},State) ->
%%     io:format("inside handle_call_delete,FileName:~p~n",[FileName]),
%% 	Reply = do_delete(FileName, From),
%%     {reply, Reply, State};


handle_call({register_replica,ChunkID,Host},{_From, _}, State)->    
    Reply = meta_common:do_register_replica(ChunkID,Host),
    {reply,Reply,State};




handle_call({getorphanchunk, HostRegName},{_From,_},State) ->
    io:format("inside handle_call_getorphanchunk,HostRegName:~p~n",[HostRegName]),
	Reply = meta_common:do_collect_orphanchunk(HostRegName),
    {reply, Reply, State};

handle_call({getfileattr, FileName},{_From,_},State) ->
    io:format("inside handle_call_getfileattr,FileName:~p~n",[FileName]),
	Reply = meta_common:do_get_fileattr(FileName),
    {reply, Reply, State};


handle_call({heartbeat,HostInfoRec},{_From,_},State) ->
  %  io:format("inside handle_call_heartbeat,HostRegName:~p~n",[HostInfoRec]),
	Reply = meta_common:do_register_heartbeat(HostInfoRec),
    {reply, Reply, State};


handle_call({nodedown,NodeName},{_From,_},State) ->
	io:format("somenode down know ~p~n",[NodeName]),
%	%Reply = do_delete_node(NodeName),
	Reply = todo,
	{reply,Reply,State};



handle_call({debug,Arg},{_From,_},State) ->
    Reply = meta_common:do_debug(Arg),
    {reply,Reply,State};

handle_call(_, {_From, _}, State)->
    io:format("inside handle_call_error~n"),
	Reply = {error, "undefined handler"},
    {reply, Reply, State}.


handle_cast({bootreport,HostInfoRec, ChunkList},{_From,_},State) ->
    io:format("inside handle_call_bootreport,HostInfoRec:~p~n",[HostInfoRec]),
	meta_common:do_dataserver_bootreport(HostInfoRec, ChunkList),
    {noreply,State}.

handle_cast(stop, State) ->
    io:format("meta server stopping~n"),
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
