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
-compile(export_all).

%%
%% API Functions
%%

solo(Des)->
    statistics(wall_clock),    
    Pid = spawn(simulate,tester,[]),
    Premote = spawn(Des,simulate,volunteer_chunk,[]),
    Premote ! {Pid,1}.
%%     {volunteer,Des}!{Pid,1}.

run(Des) ->
    statistics(wall_clock),    
    Pid = spawn(simulate,tester,[]),
    {volunteer,Des}!{Pid,1}.
       

%%
%% Local Functions
%%
prepare()->
    Pid = spawn(simulate,volunteer_chunk,[]),
    register(volunteer,Pid).

volunteer_chunk() ->
    receive 
        {Pid,Token} ->
            Pid ! {self(),Token+1},
%%              io:format("volunteer,~p~n",[Token]),
            volunteer_chunk();
        stop ->
            thank_you    
    end.

tester()->
    receive
        {Pid,Token}->
%%             io:format("in tester,~n"),
            if Token <?CHUNKSERVER_NUMBER ->
                   Pid ! {self(),Token+1},
                   io:format("Tk: ~p~n,",[Token]),
                   tester();
               true->
                   data_process(1),
                   {_,Time} = statistics(wall_clock),
                   io:format("time: ~p (millisecond)~n",[Time]),
                   Pid ! stop
            end            
    end.
    

data_process(TimeNeed) ->
%%	data_transfer,     
    timer:sleep(TimeNeed),    
%%	calculate
%%  bloom_filter_check
    ok.


start_me_up(MM,_ArgsC,_ArgS)->
    loop(MM).

loop(MM) ->
    receive
        {chan,MM,{msg,_}} ->
            loop(MM)
    end.
    
        