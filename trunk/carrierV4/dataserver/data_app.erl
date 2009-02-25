-module(data_app).
-behaviour(application).
-export([start/0, start/2, stop/1]).

start() ->
    application:start(data_app).

start(_Type, StartArgs) ->
    data_supervisor:start_link(StartArgs).

stop(_State) ->	
    ok.
