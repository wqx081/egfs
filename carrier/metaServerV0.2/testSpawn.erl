%% Author: zyb@fit
%% Created: 2009-1-13
%% test spawn function & trap_exit in different nodes.

-module(testSpawn).
%% -compile(export_all).



%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([start/0]).

%%
%% API Functions
%%

start()->
    
    io:format("testcase: start(false, abc).~n"),
    starttest(false, abc),
    io:format("~ntestcase: start(false, normal).~n"),
    starttest(false, normal),
    io:format("~ntestcase: start(false, die)..~n"),
    starttest(false, die),
    io:format("~ntestcase: start(false, kill).~n"),
    starttest(false, kill),
    io:format("~ntestcase: start(true, abc)..~n"),
    starttest(true, abc),
    io:format("~ntestcase: start(true, normal)..~n"),
    starttest(true, normal),
    io:format("~ntestcase: start(true, kill)..~n"),
    starttest(true, kill).

starttest(Bool, M) ->
    %% Spawn three process A B and C
    A = spawn(fun() -> a() end),
    
    
    
%%      register(ReadAtom,  spawn(fileMan,readProcess,[FileID])    ),
    
    
%%      B = spawn(fun() -> b(A, Bool) end),
    
    {_, HighTime, LowTime}=now(),
    
    B = spawn(fileWorker,writeProcessTest,[<<HighTime:32, LowTime:32>>,A,Bool]),
    
    global:register_name(fileWriteP,B),
    
    C = spawn(fun() -> c(B, M) end),

    sleep(1000),
    status(a, A),
    status(b, B),
    status(c, C).

a() ->      
    process_flag(trap_exit, true),
    wait(a).

b(A, Bool) ->
    process_flag(trap_exit, Bool),
    link(A),
    wait(b).

c(B, M) ->
    process_flag(trap_exit, true),
    link(B),
    exit(B, M),
    wait(d).


wait(Prog) ->
    receive
	Any ->
	    io:format("Process ~p received ~p~n",[Prog, Any]),
	    wait(Prog)
    end.

sleep(T) ->
    receive
    after T -> true
    end.

status(Name, Pid) ->	    
    case process_info(Pid) of
	undefined ->
	    io:format("process ~p (~p) is dead~n", [Name,Pid]);
	_ ->
	    io:format("process ~p (~p) is alive~n", [Name, Pid])
    end.



%%
%% Local Functions
%%

