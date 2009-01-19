-module(heart_beat_report).
-include("../include/egfs.hrl").
-include("data_server.hrl").
-export([start/0,stop/0]).


start() ->
    register(heart_beater, spawn_link(fun() -> loop(?HEART_BEAT_PERIOD) end)).

stop() ->
    heart_beater ! stop.

loop(Time) ->
    receive
	stop ->
	    void
    after Time -> 
	    {ok, HostInfoRec} = boot_report:get_host_info(),
	    try gen_server:call(?META_SERVER,{heartbeat,HostInfoRec}) of
		{ok, _Any} ->
		    void;
		{error, _Any} ->
		    boot_report:boot_report();
		_Any ->
		    void
	    catch
		exit:_  ->
		    toolkit:sleep(?HEART_BEAT_REPORT_WAIT_TIME),
		    loop(Time);
		  _Any ->
		    void
	    end,
	    loop(Time)
    end.
    
