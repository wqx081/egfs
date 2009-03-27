%% Author: zyb@fit
%% Created: 2009-3-9
%% Description: TODO: Add description to simulate
-module(simulate).

%%
%% Include files
%%
-include("data.hrl").
%%
%% Exported Functions
%%
%% -export([]).
-compile(export_all).

%%
%% API Functions
%%

run()->
    
    B001 = lib_bloom:new(?TOTAL_CHUNK_RECORD_NUMBER,0.01),
    Bloom_bits =element(2,B001),
    TransferTime = Bloom_bits/?BANDWIDTH/1024/1024*1000,	%%millisecond
    RoundTransferTime = round(TransferTime),
    io:format("transfer time:~p.~n",[RoundTransferTime]),
    
    %%
    
    {_,BS,BM} = now(),
    self()!{BS,BM},
    chunk_server(1,RoundTransferTime,self()),     
    receive 
        {job_ok,ID}->
            io:format("last chunk ~p ok. job finish.~n",[ID])
    after 50000 ->
            io:format("time out~n")    
    end,
    {_,ES,EM} = now(),
    io:format("---------------------------------------------~n"),
    io:format("job begin time, second:~p , microsecond ~p ~n",[BS,BM]),
    io:format("job  end  time, second:~p , microsecond ~p ~n",[ES,EM]),
    io:format("chunck server number: ~p~n",[?CHUNKSERVER_NUMBER]),
    if EM>BM ->
           io:format("job take time: ~ps,~pms.~n",[ES-BS,EM-BM]);
       true ->
           io:format("job take time: ~ps,~pms.~n",[ES-BS-1,EM-BM+1000000])
    end.

    

%%
%% Local Functions
%%


%%chunkserver transfer data to next chunk after latency of ?LATENCY , 
%% @spec chunck_server(ID) -> ()  when  1<= ID < 2048, calc and start next node.

chunk_server(ID,T,P) when ID < ?CHUNKSERVER_NUMBER ->
    timer:sleep(1), %%data receive latency.    
    _Pid = spawn(simulate,data_process,[ID,T]),
%%     chunk_server(ID+1,T,P);
     _Pid2 = spawn(simulate,chunk_server,[ID+1,T,P]);    

chunk_server(ID,T,P) ->
    timer:sleep(?LATENCY), %%data receive latency.    
    data_process(ID,T),
%%     receive 
%%         {BS,BM}->
%%             io:format("last chunk ~p ok. job finish.~n",[ID]);
%%         Any ->
%%             io:format("a?  ~p.~n",[Any])    
%%     after 0 ->
%%             io:format("time out~n")    
%%     end,
    
    P!{job_ok,ID}.
                   

data_process(ID,_TimeNeed) ->
%%	data_transfer,     
%%     timer:sleep(TimeNeed),    
%%	calculate
%%  bloom_filter_check    
    {job,ID,ok}.
    
    
    



%%test

test()->
  	
    statistics(wall_clock),    
    
    for(1,1000,fun(I)->one_milisecond_function(I) end),
    
    {_,Time} = statistics(wall_clock),    
    io:format("time,we want 1000: ~p.~n",[Time]).
    
%%     timer:sleep(1),%%15millisecond,

    

test3()->
    io:format("current time:~p~n",[now()]),
    timer:apply_after(5000, simulate, test2, []),
	timer:sleep(2000),
    io:format("sllep over.~n"),
    io:format("current time:~p~n",[now()]).

test2()->
	io:format("test out put.. now:~n"),
    io:format("current time:~p~n",[now()]).
    

for(Max, Max, F) -> [F(Max)];
for(I, Max, F)   -> [F(I)|for(I+1, Max, F)].

one_milisecond_function(N)->
    for(1,2900,fun(I)->I*I end).
    