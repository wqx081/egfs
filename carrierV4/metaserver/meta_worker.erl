%%%-------------------------------------------------------------------
%%% File    : meta_worker.erl
%%% Author  : Xiaomeng Huang
%%% Description : Metadata Server
%%%
%%% Created :  30 Jan 2009 by Xiaomeng Huang
%%%-------------------------------------------------------------------
-module(meta_worker).
-behaviour(gen_server).
-include("../include/header.hrl").

-import(meta_db,[
                  select_all_from_filemeta/1,
                  select_nodeip_from_chunkmapping/1
                ]).


-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([
         try_close/1
         ]).


-record(metaWorkerState,{filemeta=#filemeta{},mod,clients=[]}).


%%====================================================================
%% gen_server callbacks
%%====================================================================
init([FileRecord,Mod]) ->
	process_flag(trap_exit,true),
	error_logger:info_msg("[~p, ~p]: start metaworker ~p~n", [?MODULE, ?LINE, self()]),
    
    {ok,_Tref} = timer:apply_interval(100000,meta_worker,try_close,[FileRecord#filemeta.fileid]), % check host health every 5 second
    
	State=#metaWorkerState{filemeta=FileRecord,mod=Mod,clients=[]},
    {ok, State}.

handle_call({registerchunk,FileRecord, ChunkMappingRecords}, {_From, _}, State) ->
    error_logger:info_msg("~~~~ in registerchunk~~~~n"),
    io:format("checking...~n"),
    io:format("worker state:~n"),
    io:format("mod:    ~p~n",[State#metaWorkerState.mod]),
    io:format("FILEID: ~p~n",[(State#metaWorkerState.filemeta)#filemeta.fileid]),
    io:format("submit: ~p~n",[FileRecord#filemeta.fileid]),
	Reply = meta_db:add_a_file_record(FileRecord, ChunkMappingRecords),
    
%%    Reply = do_register_chunk(FileID, ChunkID, ChunkUsedSize, NodeList),
	{reply, Reply, State};

handle_call({seekchunk, ChunkID}, {_From, _}, State) ->
	Reply = meta_db:select_hosts_from_chunkmapping_id(ChunkID),
	{reply, Reply, State};
	
handle_call({getfileinfo,FileName}, {_From, _}, State) ->     
	
    Reply  = meta_db:select_all_from_filemeta_byName(FileName),
	{reply, Reply, State};


handle_call({locatechunk,FileID, ChunkIndex}, {_From, _}, State) ->
    Reply = do_get_chunk(FileID, ChunkIndex),
    
	{reply, Reply, State};


handle_call({debug},{_From, _},State)->    
    io:format(" someone is checking my state.~n"),
    {reply,{self(),State},State}.


%% removed?
%% handle_call({allocatechunk,FileID}, {From, _}, State) ->
%%     Reply = do_allocate_chunk(FileID,From),
%%     {reply, Reply, State}.


handle_cast({stop,_Reason,From}, State) ->
    %%TODO.
    do_close(From,State),
	{noreply, State};	
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', _Pid, Why}, State) ->
    io:format("EXiT.~n"),
	{stop, normal, State};	
handle_info(_Info, State) ->
    io:format("handle_info.~n"),
    {noreply, State}.

terminate(Reason, _State) ->
	error_logger:info_msg("[~p, ~p]: close metaworker ~p since ~p~n", [?MODULE, ?LINE, self(),Reason]),	
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% 				do_

do_get_chunk(FileID, ChunkIdx)->
    case select_all_from_filemeta(FileID) of
        []->
            {error,"file does not exist"};
        [#filemeta{chunklist = ChunkList}]->
            if
                (length(ChunkList) =< ChunkIdx) ->
                    {error, "chunkindex is larger than chunklist size"};
                true ->
                    ChunkID = lists:nth(ChunkIdx+1, ChunkList),
                    case select_nodeip_from_chunkmapping(ChunkID) of
                        [] -> 
                            {error, "chunk does not exist"};
                        [ChunkLocations] ->
                            {ok, ChunkID, ChunkLocations}
                    end
            end
    end.


do_close(From,State) ->
    error_logger:info_msg("-- meta_worker  do_close"),
    case State#metaWorkerState.mod of
        read->
            
            Clients = State#metaWorkerState.clients--[From],
            error_logger:info_msg("mod = r , Clients = ",[Clients]),
            case Clients of
                []->
                    exit(normal);    %%use handle_info instead of handle_cast, avoid crash
                _ ->
                    nil
            end;
        write->
            error_logger:info_msg("mod = w"),
            exit(normal)
    end.

try_close(ID) ->
    io:format("trying to close this worker(FileID: ~p ). every 100s~n",[ID]).
    

