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
                  select_all_from_filemeta/1                  
                ]).


-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%% -export([
%%          try_close/1
%%          ]).


-record(metaWorkerState,{filemeta=#filemeta{},mod,clients=[]}).


%%====================================================================
%% gen_server callbacks
%%====================================================================
init([FileRecord,Mod,_UserName]) ->
	process_flag(trap_exit,true),
%% 	error_logger:info_msg("[~p, ~p]: start metaworker ~p~n", [?MODULE, ?LINE, self()]),
    
%%     {ok,_Tref} = timer:apply_interval(100000,meta_worker,try_close,[FileRecord#filemeta.id]), % check host health every 5 second
    
	State=#metaWorkerState{filemeta=FileRecord,mod=Mod,clients=1},
    {ok, State}.

handle_call({registerchunk,FileRecord, ChunkMappingRecords}, {_From, _}, State) ->
    error_logger:info_msg("~~~~ in registerchunk~~~~n"),    
    error_logger:info_msg("FileRecord:~p ,ChunkMappingRecords:~p ,mod:    ~p,FILEID: ~p,submit: ~p ~n",
                          [FileRecord,ChunkMappingRecords,State#metaWorkerState.mod,(State#metaWorkerState.filemeta)#filemeta.id,FileRecord#filemeta.id]),
    case State#metaWorkerState.mod of
        write ->
            Reply = meta_db:add_a_file_record(FileRecord, ChunkMappingRecords);
        append ->
            Reply = meta_db:append_a_file_record(FileRecord, ChunkMappingRecords);
        Any->
            Reply = {error,"mode error while registerchunk,~p~n",[Any]}
    end,
%%    Reply = do_register_chunk(FileID, ChunkID, ChunkUsedSize, NodeList),
	{reply, Reply, State};

handle_call({seekchunk, ChunkID}, {_From, _}, State) ->
%%     error_logger:info_msg("~~~~ in seekchunk~~~~n"),
	[Reply] = meta_db:select_hosts_from_chunkmapping_id(ChunkID),   %% select result = [{},{}]
	{reply, Reply, State};
	

%% when a worker of mod "read" was created, clients is initialized to 0
handle_call({joinNewReader}, {_From, _}, State) ->     
%% 	error_logger:info_msg("~~~~ in getfileinfo~~~~n"),    
    NewState = State#metaWorkerState{clients=State#metaWorkerState.clients+1},
    Reply = State#metaWorkerState.filemeta,    
%%     Reply  = meta_db:select_all_from_filemeta_byName(FileName),
    
	{reply, Reply, NewState};


handle_call({locatechunk,FileID, ChunkIndex}, {_From, _}, State) ->
%%     error_logger:info_msg("~~~~ in locatechunk~~~~n"),
    Reply = do_get_chunk(FileID, ChunkIndex),
    
	{reply, Reply, State};


handle_call({debug},{_From, _},State)->    
    error_logger:info_msg(" someone is checking my state.~n"),
    {reply,{self(),State},State}.


%% removed?
%% handle_call({allocatechunk,FileID}, {From, _}, State) ->
%%     Reply = do_allocate_chunk(FileID,From),
%%     {reply, Reply, State}.

%% 
%% handle_cast({stop,_Reason,From}, State) ->
%% %%     error_logger:info_msg("Reason: ~p~n",[Reason]),
%% %%     error_logger:info_msg("State: ~p~n",[State]),
%%     %%TODO.
%%     
%%     end
%%     
%%     case do_close(From,State) of
%%         readerleave->
%%             error_logger:info_msg("[~p, ~p]: one reader exist, ~n", [?MODULE, ?LINE]),
%%             NewState = State#metaWorkerState{clients=State#metaWorkerState.clients-1},
%%             error_logger:info_msg("[~p, ~p]: reader online: ~p ~n", [?MODULE, ?LINE,NewState#metaWorkerState.clients]),
%%             {noreply, NewState};        
%%         Other->
%%             error_logger:info_msg("[~p, ~p]: other: ~p,  ~n", [?MODULE, ?LINE,Other]),
%%             {noreply,State}
%%     end;
%% 
%% handle_cast({close,_Reason,From}, State) ->
%% %%     error_logger:info_msg("Reason: ~p~n",[Reason]),
%% %%     error_logger:info_msg("State: ~p~n",[State]),
%%     %%TODO.
%%     
%%     case do_close(From,State) of
%%         readerleave->
%%             error_logger:info_msg("[~p, ~p]: one reader exist, ~n", [?MODULE, ?LINE]),
%%             NewState = State#metaWorkerState{clients=State#metaWorkerState.clients-1},
%%             error_logger:info_msg("[~p, ~p]: reader online: ~p ~n", [?MODULE, ?LINE,NewState#metaWorkerState.clients]),
%%             {noreply, NewState};        
%%         Other->
%%             error_logger:info_msg("[~p, ~p]: other: ~p,  ~n", [?MODULE, ?LINE,Other]),
%%             {noreply,State}
%%     end;

handle_cast(_Msg, State) ->    
    {noreply, State}.

handle_info({'EXIT', _Pid, Why}, State) ->   %%
     error_logger:info_msg("[~p, ~p]: EXiT. Why: ~p~n",[?MODULE, ?LINE,Why]),     
     NewState = State#metaWorkerState{clients=State#metaWorkerState.clients-1},     
     case NewState#metaWorkerState.clients of
         0 ->
             {stop,Why,NewState};
         _C ->
             {noreply,NewState}	%%TODO: reader client exit
     end;

handle_info(_Info, State) ->
%%     error_logger:info_msg("handle_info.~n"),
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
                    case meta_db:select_hosts_from_chunkmapping_id(ChunkID) of
                        [] -> 
                            {error, "chunk does not exist"};
                        [ChunkLocations] ->
                            {ok, ChunkID, ChunkLocations}
                    end
            end
    end.


%% do_close(_From,State) ->
%%      error_logger:info_msg("-- meta_worker  do_close"),
%%      error_logger:info_msg(" metaworkerstate.clients : ~p~n",[State#metaWorkerState.clients]),
%% %%     error_logger:info_msg("From: ~p~n",[From]),
%% 
%%     case State#metaWorkerState.mod of
%%         read->                      
%% %%             Clients = State#metaWorkerState.clients--[From],
%%             Clients = State#metaWorkerState.clients-1,
%% %%             error_logger:info_msg("mod = read , Clients = ",[Clients]),
%%             case Clients of
%%                 0->
%%                     exit(normal);    %%use handle_info instead of handle_cast, avoid crash
%%                 _ ->
%%                     readerleave
%%             end;
%%         write->
%% %%             error_logger:info_msg("mod = write"),
%%             exit(normal);
%%         append->
%%             exit(normal)
%%     end.

%% try_close(ID) ->
%%     error_logger:info_msg("trying to close this worker(FileID: ~p ). every 100s~n",[ID]).
    

