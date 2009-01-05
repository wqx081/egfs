-module(app_meta_server).
-behaviour(application).
-export([start/0, start/2, stop/1]).

%% start according to app config file metaserver.app
start() ->
    application:start(meta_server). 

start(_Type, StartArgs) ->
    io:format("meta server app is starting~n"),
    supervisor_meta_server:start_link(StartArgs).

stop(_State) ->	
    io:format("meta server app is stopping~n"),
    ok.
