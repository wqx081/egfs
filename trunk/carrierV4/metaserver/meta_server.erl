-module(meta_server).
-behaviour(gen_server).

-include("../include/header.hrl").

-import(meta_db,[
                  select_fileid_from_filemeta/1,
                 select_from_hostinfo/1,
                 select_chunkid_from_orphanchunk/1,
                 detach_from_chunk_mapping/1,
                 do_register_dataserver/2,
                 do_delete_filemeta/1,
                 do_delete_orphanchunk_byhost/1,
                 select_attributes_from_filemeta/1,
                  start_mnesia/0
                ]).

-export([start/0,stop/0]).
-export([init/1, handle_call/3, handle_cast/2,handle_info/2,terminate/2, code_change/3]).
-export([open/2,writeAllocate/1,locateChunk/2,registerChunk/4,close/1]).
-compile(export_all).

init(_Arg) ->
    process_flag(trap_exit, true),
    io:format("meta server starting~n"),
    %init_mnesia(),
    {ok, []}.

start() ->
    start_mnesia(),
%%     {ok,Tref} = timer:apply_interval((?HEART_BEAT_TIMEOUT),hostMonitor,checkHostHealth,[]), % check host health every 5 second
    gen_server:start_link(?META_SERVER, meta_server, [], []).

stop() ->
    gen_server:cast(?META_SERVER, stop).

terminate(_Reason, _State) ->
    io:format("meta server terminating~n").


%"metaserver" methods
% write step 1: open file
handle_call({open, FileName, Mode}, {From, _}, State) ->
    io:format("inside handle_call_open, FileName:~p,Mode:~p,From:~p~n",[FileName,Mode,From]),
    Reply = do_open(FileName, Mode, From),
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

handle_call({delete,FileName},{From,_},State) ->
    io:format("inside handle_call_delete,FileName:~p~n",[FileName]),
	Reply = do_delete(FileName, From),
    {reply, Reply, State};


handle_call({register_replica,ChunkID,Host},{From, _}, State)->
    %TODO.
    Reply = todo,
    {reply,Reply,State}.


handle_call({bootreport,HostInfoRec, ChunkList},{_From,_},State) ->
    io:format("inside handle_call_bootreport,HostInfoRec:~p~n",[HostInfoRec]),
	Reply = do_dataserver_bootreport(HostInfoRec, ChunkList),
    {reply, Reply, State};

handle_call({getorphanchunk, HostRegName},{_From,_},State) ->
    io:format("inside handle_call_getorphanchunk,HostRegName:~p~n",[HostRegName]),
	Reply = do_collect_orphanchunk(HostRegName),
    {reply, Reply, State};

handle_call({getfileattr, FileName},{_From,_},State) ->
    io:format("inside handle_call_getfileattr,FileName:~p~n",[FileName]),
	Reply = do_get_fileattr(FileName),
    {reply, Reply, State};


handle_call({heartbeat,HostInfoRec},{_From,_},State) ->
  %  io:format("inside handle_call_heartbeat,HostRegName:~p~n",[HostInfoRec]),
	Reply = do_register_heartbeat(HostInfoRec),
    {reply, Reply, State};


handle_call({debug,Arg},{_From,_},State) ->
    Reply = do_debug(Arg),
    {reply,Reply,State};

handle_call(_, {_From, _}, State)->
    io:format("inside handle_call_error~n"),
	Reply = {error, "undefined handler"},
    {reply, Reply, State}.


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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5
%%from old meta_server ;   handle these function.

do_open(FileID,Mod,_From)-> 
    case Mod of
        r->            
            do_read_open(FileID);
        w->
            do_write_open(FileID);
        _->
            {error,"unkown open mode"}
    end.

do_write_open(FileID)->
    ProcessName = util:idToAtom(FileID,w),
    case whereis(ProcessName) of
        undefined -> % no meta worker , create one worker to server this writing request.
			case meta_db:select_all_from_filemeta(FileID) of
				[] ->
					% if the target file is not exist, then generate a new fileid.
                    Filename = todo,  %%TODO                 
				    FileRecord = #filemeta{	fileid=FileID, 
											filename=Filename, 
											filesize=0, 
											chunklist=[], 
				                 			createT=erlang:localtime(), 
											modifyT=erlang:localtime(),
											acl="acl"},
					{ok, MetaWorkerPid}=gen_server:start({local,ProcessName}, meta_worker, [FileRecord], []),
					{ok, FileID, 0, [], MetaWorkerPid};	  
				[_FileMeta] ->	
					{error, "the same file name has existed in database."}
			end;
        _Pid->			% pid exist, write error
            {error, "other client is writing the same file."}  
    end.

do_read_open(FileID)->
    ProcessName = util:idToAtom(FileID,r),
    case whereis(ProcessName) of
        undefined ->		% no meta worker , create one worker to server this writing request.
			case meta_db:select_all_from_filemeta(FileID) of
				[] ->
					% if the target file is not exist, then report error .		
					{error, "the target file is not exist."};  
				[FileMeta] ->	 %%%%%%%%%%%%%%%%%%%%% wait for modifying 
					{ok, MetaWorkerPid}=gen_server:start({local,ProcessName}, meta_worker, [FileMeta], []),
					{ok, FileMeta#filemeta.fileid, FileMeta#filemeta.filesize, FileMeta#filemeta.chunklist, MetaWorkerPid}
			end;
        Pid->			% pid exist, set this pid process as the meta worker
        	FileMeta = gen_server:call(Pid, {getfileinfo}),
       		{ok, FileMeta#filemeta.fileid, FileMeta#filemeta.filesize, FileMeta#filemeta.chunklist, Pid}
   end.



%% read file attribute step 1: open file
%% read file attribute step 2: get chunk for 
do_get_fileattr(FileName)->
    [AttributeList] =select_attributes_from_filemeta(FileName),
    {ok, AttributeList}.

%%
%% 
%% host drop.
%% update chunkmapping and delete chunks
%% arg -  that host
%% return ---
do_detach_one_host(HostName)->
    detach_from_chunk_mapping(HostName). %metaDB fun


do_dataserver_bootreport(HostRecord, ChunkList)->
     do_register_dataserver(HostRecord, ChunkList). %metaDB fun

%% 
%%delete 
do_delete(FileName, _From)->
    case select_fileid_from_filemeta(FileName) of
        [] -> {error, "filename does not exist"};
        % get fileid sucessfull	
        [FileID] ->
            do_delete_filemeta(FileID)
    end.

%%
%% 
%%
do_collect_orphanchunk(HostProcName)->
    % get orphanchunk from orphanchunk table
    OrphanChunkList = select_chunkid_from_orphanchunk(HostProcName),
    % delete notified orphanchunk from orphanchunk table
    do_delete_orphanchunk_byhost(HostProcName),
    OrphanChunkList.

do_register_heartbeat(_HostInfoRec)->
    %%TODO:
    
    {ok,"heartbeat Ok"}
    .
%%     
%%     Res = select_from_hostinfo(HostInfoRec#hostinfo.hostname),
%%     Host = HostInfoRec#hostinfo.host,
%%     case Res of
%%         [{hostinfo,_Proc,Host,_TotalSpace,_FreeSpace,_Health}]->
%%             {_,TimeTick,_} = now(),
%%             NewInfo = HostInfoRec#hostinfo{health = {TimeTick,10}}, %%   ?INIT_NODE_HEALTH = 10 , TODO, 
%%             write_to_db(NewInfo),
%%             {ok,"heartbeat ok"};                
%%         _-> {error,"chunk does not exist"}
%%     end.

rpc(Pid, Request) ->
    Pid ! {self(), Request},
    receive
	Response ->
	    Response
    end.


%% 
%% debug 
%% 
do_debug(Arg) ->
    io:format("in func do_debug~n"),
    case Arg of
        wait ->
            io:format("111 ,~n"),
            timer:sleep(2000),
			io:format("222 ,~n");
        clearShadow ->
            mnesia:clear_table(filemeta_s);
        show ->
            io:format("u wana show, i give u show ,~n");
        die ->
            io:format("u wna die ,i let u die.~n")
%%             1000 div 0;
            ;
        _ ->
            io:format("ohter~n")
    end.




%% --------------------------------------------------------------------------------------------------
%% for name_server 
%% 
call_meta_open(FileID, Mode)->
    do_open(FileID, Mode,[]).


call_meta_new() ->
    {_, HighTime, LowTime}=now(),
    FileID = <<HighTime:32, LowTime:32>>,
    {ok, FileID}.




call_meta_delete(FileID)->
    do_delete_filemeta(FileID).


call_meta_copy(_FileID) ->
 
%%     gen_server:call(?CHUNK_SERVER,{chunkCopy,[From],[To1,To2]}),
    
    {ok, <<1:64>>}
.

call_meta_check(list,[]) ->
    {ok,"no file conflict"};
call_meta_check(list,FileIDList)->   %% return ok || ThatFileID
    io:format("aaa,~p~n",[FileIDList]),
    [FileID|T] = FileIDList,
    case call_meta_check(FileID) of
        {ok,_}->
            call_meta_check(list,T);
        {error,_}->
            {error,FileID}
    end.

call_meta_check(FileID)->
    WA = meta_util:idToAtom(FileID,w),
    RA = meta_util:idToAtom(FileID,r),
    
    case whereis(WA) of
        undefined->
            case whereis(RA) of
                undefined->
                    {ok,FileID};
                _->
                    {error,"someone reading this file."}
            end;
        _->
            {error,"someone writing this file."}
    end.




