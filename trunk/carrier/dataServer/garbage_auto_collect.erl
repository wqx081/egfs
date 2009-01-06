-module(garbage_auto_collect).
-include("../include/egfs.hrl").
-compile(export_all).

-define(GARBAGE_AUTO_COLLECT_PERIOD,	7000).    % 5000 milisecond = 5 second

start() ->
    register(garbage_auto_collector, spawn_link(fun() -> loop(?GARBAGE_AUTO_COLLECT_PERIOD) end)).

stop() ->
    garbage_auto_collector ! stop.

loop(Time) ->
    receive
	stop ->
	    io:format("garbage auto collector stopped ~n")
    after Time -> 
	    io:format("garbage auto collector  event~n"),
	    {ok, HostProcName} = boot_report:get_proc_name(),
	    case gen_server:call(?META_SERVER, {getorphanchunk, HostProcName}) of
		 {ok, OrphanChunkList} ->
		    chunk_garbage_collect:collect(OrphanChunkList);
		_Any ->
		    io:format("garbage auto collector failed ~p ~n",[_Any])
	    end,
	    loop(Time)
    end.
    
