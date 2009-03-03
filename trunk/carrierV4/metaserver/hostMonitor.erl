%% Author: zyb@fit
%% Created: 2009-1-4
%% Description: TODO: Add description to hostMonitor
-module(hostMonitor).

%%
%% Include files
%%
-include("../include/header.hrl").
-import(lists, [foreach/2]).
-import(meta_db,[select_all_from_Table/1,
                 delete_object_from_db/1,
                 detach_from_chunk_mapping/1,               
                 select_from_hostinfo/1,                
                 write_to_db/1]).
%%
%% Exported Functions
%%
%%-export([hello/0,checkHostHealth/0,test/0]).
-export([checkNodes/0,broadcast/0]).

%%
%% API Functions
%%

%apply_after(Time,Module,Function,Arguments)


%%
%% Local Functions
%%
test() ->
    {ok,Tref} = timer:apply_interval(1000,hostMonitor,hello,[]).


hello() ->
%%     timeCheck(10),
%%     timeCheck(4),
	io:format("hello,,~p~n",[now()]).



%%% check node status
checkNodes() ->
    receive 
        {nodedown,Node} ->			%%TODO, Maybe we shall improve this.
            X = select_from_hostinfo(Node),
            delete_object_from_db(X),
            detach_from_chunk_mapping(X#hostinfo.hostname),
            {nodedown_deleted,X};
        Any ->
            Any
    after 0 ->
            {all_node_ok,[]}
    end.            


%% broadcast metainfo.
%% metaserver choose one host , let him do the rest
%%%%%%%

broadcast() ->
    Mappings = select_all_from_Table(chunkmapping),
    MappingsNum = length(Mappings),
    if         
        (MappingsNum =< 0) ->
            {erroe, "no chunkid in chunkmapping table"};
        true ->
            ChunkIdList = get_chunkid_from_chunkmapping(Mappings),
            ChunkMapping = lists:nth(1,Mappings),
            FirstHost =lists:nth(1,ChunkMapping#chunkmapping.chunklocations), 
            %% use lib_chan & lib_bloom
            
            todo
            %%code:add_patha("d:/EclipseWorkS/edu.tsinghua.carrier/carrierV4/lib"),            
            %%{ok, DataWorkPid} = lib_chan:connect(FirstHost, ?DATA_PORT, dataworker,?PASSWORD,  {garbageCollect})
            %%TODO:
            
            

            
    end.
            
            
%% function for broadcast,
%% @spect get_chunckid_from_chunkmapping( Node#chunkmapping ) -> ChunkIDList :[chunk1,chunk2]
get_chunkid_from_chunkmapping(Nodes) ->
    [H|T] = Nodes,
    [H#chunkmapping.chunkid]++get_chunkid_from_chunkmapping(T).


%%old.
%% 
%% checkHostHealth()->    
%%     X = select_all_from_Table(hostinfo),
%% 	foreach(fun checkThatHost/1,X).
%% 
%% 	% X#hostinfo = {hostinfo,{data_server,lt@lt},{192,168,0,111},1000000,2000000,{0,100}}
%% checkThatHost(X)->
%%     
%%     S = X#hostinfo.status,
%%     {_,Time,_} = now(),
%%     C  = (Time-LastHeartBeat)*1000,  % C milisecond
%%     case timeCheck(C) of        
%%         false->
%%             NewCounter = Counter-1,            
%%             if NewCounter<0 ->   % host die
%%                    delete_object_from_db(X),
%%                    detach_from_chunk_mapping(X#hostinfo.procname);
%%                true ->
%%                    NewX = X#hostinfo{health={LastHeartBeat,NewCounter}},
%%                    write_to_db(NewX)           
%%             end;
%%         _ ->
%%             ok
%% 	end.
%%     
%% timeCheck(C)->
%%     if C < (?HEART_BEAT_TIMEOUT), C >= 0 ->
%%            true;        
%%        true ->
%%            false
%%     end.

    
    
    

