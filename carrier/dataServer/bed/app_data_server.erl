-module(app_data_server).
-behaviour(application).
-export([start/0, start/2, stop/1]).

start() ->
    application:start(data_server).

start(_Type, StartArgs) ->
    io:format("data_server app is starting~n"),
    supervisor_data_server:start_link(StartArgs).

stop(_State) ->	
    io:format("data_server app is stopping~n"),
    ok.
