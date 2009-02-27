-module(client_supervisor).
-behaviour(supervisor).
-export([start/0, start_link/1,	 start_in_shell/0, init/1]).

start() ->
    spawn(fun() ->
		supervisor:start_link( {local, ?MODULE}, ?MODULE, _Arg = [])
	    end).

start_in_shell() ->
    {ok, Pid} = supervisor:start_link( {local, ?MODULE}, ?MODULE, _Arg = []),
    unlink(Pid).

start_link(Args) ->
    supervisor:start_link( {local, ?MODULE}, ?MODULE, Args).



init([]) ->
    AChild = {tag1,{client,start_link,[]}, permanent,2000,worker,[client_server]},
    {ok,{{one_for_one, 3, 10}, [AChild]}}.


