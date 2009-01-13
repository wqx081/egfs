-module(heart_beat_report).
-include("../include/egfs.hrl").
-export([start/0,stop/0]).


start() ->
    register(heart_beater, spawn_link(fun() -> loop(?HEART_BEAT_PERIOD) end)).

stop() ->
    heart_beater ! stop.

loop(Time) ->
    receive
	stop ->
	    io:format("heart beat stopped ~n")
    after Time -> 
	    io:format("heart beat event~n"),
	    {ok, HostInfoRec} = boot_report:get_host_info(),
	    case gen_server:call(?META_SERVER,{heartbeat,HostInfoRec}) of
		{ok, _} ->
		    io:format("heart beat report succeeded ~n");
		{error, _} ->
		    io:format("heart beat report failed ~n"),
		    boot_report:boot_report();
		_Any ->
		    void
	    end,
	    loop(Time)
    end.
    
