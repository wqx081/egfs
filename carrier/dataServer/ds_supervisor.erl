-module(ds_supervisor).
-behaviour(supervisor).
-include("../include/egfs.hrl").
-export([start/0, init/1,
	 start_in_shell/0]).
-define(NAME, {local, ?MODULE}).

start() ->
    spawn(fun() ->
		supervisor:start_link(?NAME, ?MODULE, _Arg = [])
	    end).

start_in_shell() ->
    {ok, Pid} = supervisor:start_link(?NAME, ?MODULE, _Arg = []),
    unlink(Pid).

init([]) ->
    ?DEBUG("starting data server supervisor~n", []),
    {ok, {{one_for_one, 3, 10},
	   [{data_server, 
	       {data_gen_server, start, []},
	       permanent,
	       10000,
	       worker,
	       [data_gen_server]}
	   ]}}.

