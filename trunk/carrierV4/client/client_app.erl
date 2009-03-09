-module(client_app).
-behaviour(application).
-export([start/0, start/2, stop/1]).

start() ->
    application:start(client_app).    

start(_Type, StartArgs) ->
	{ok, Hosts} = application:get_env(client_app, metaserver),
	{ok, WaitTime} = application:get_env(client_app, waittime),	
	net_adm:world_list(Hosts),
	%net_adm:ping(ms@hxm),
	timer:sleep(timer:seconds(WaitTime)),
    client_supervisor:start_link(StartArgs).

stop(_State) ->	
    ok.
