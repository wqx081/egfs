%% Author: zyb@fit
%% Created: 2009-1-4
%% Description: TODO: Add description to hostMonitor
-module(hostMonitor).

%%
%% Include files
%%
-include("../include/egfs.hrl").
-import(lists, [foreach/2]).
-import(metaDB,[select_all_from_Table/1,
                delete_object_from_db/1,
                detach_from_chunk_mapping/1,
                write_to_db/1]).
%%
%% Exported Functions
%%
-export([hello/0,checkHostHealth/0,test/0]).

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


checkHostHealth()->    
    X = select_all_from_Table(hostinfo),
	foreach(fun checkThatHost/1,X).

	% X#hostinfo = {hostinfo,{data_server,lt@lt},{192,168,0,111},1000000,2000000,{0,100}}
checkThatHost(X)->
    {LastHeartBeat,Counter} = X#hostinfo.health,
    {_,Time,_} = now(),
    C  = (Time-LastHeartBeat)*1000,  % C milisecond
    case timeCheck(C) of        
        false->
            NewCounter = Counter-1,            
            if NewCounter<0 ->   % host die
                   delete_object_from_db(X),
                   detach_from_chunk_mapping(X#hostinfo.procname);
               true ->
                   NewX = X#hostinfo{health={LastHeartBeat,NewCounter}},
                   write_to_db(NewX)           
            end;
        _ ->
            ok
	end.
    
timeCheck(C)->
    if C < (?HEART_BEAT_TIMEOUT), C >= 0 ->
           true;        
       true ->
           false
    end.