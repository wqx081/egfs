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
%%     io:format("current time:~p.~n",[now()]),
    {_,BS,BM} = now(),
    _Res = chunk_server(2048),    
%%     io:format("~p~n",[Res]),
%%     timer:sleep(1000),    
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

chunk_server(ID) when ID < ?CHUNKSERVER_NUMBER ->
    timer:apply_after(?LATENCY,simulate,chunk_server,[ID+1]),
%%     Time = ?TOTAL_CHUNK_RECORD_NUMBER*?BLOOM_RECORD_SIZE/?BANDWIDTH/1000000,
%%     timer:sleep(round(Time-?LATENCY)),
    {server_ok,ID};

chunk_server(ID) ->
    Time = ?TOTAL_CHUNK_RECORD_NUMBER*?BLOOM_RECORD_SIZE/?BANDWIDTH/1000000*1000,    
%%     io:format("Time =  ~p~n",[Time]),
    timer:sleep(round(Time)),
%%     io:format("current time:~p.~n",[now()]),
    {job_ok,ID}.
                   

do_transfer(ID,TimeNeed) ->
    timer:sleep(TimeNeed),
    {job,ID,ok}.
    
    
    



%%test
test()->
    io:format("current time:~p~n",[now()]),
    timer:apply_after(5000, simulate, test2, []),
	timer:sleep(2000),
    io:format("sllep over.~n"),
    io:format("current time:~p~n",[now()]).

test2()->
	io:format("test out put.. now:~n"),
    io:format("current time:~p~n",[now()]).
    


