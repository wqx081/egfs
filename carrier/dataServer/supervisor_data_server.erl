-module(supervisor_data_server).
-behaviour(supervisor).
-include("../include/egfs.hrl").
-export([start/0, 
	 start_link/1,
	 start_in_shell/0,
	 init/1]).

-define(NAME, {local, ?MODULE}).

start() ->
    spawn(fun() ->
		supervisor:start_link(?NAME, ?MODULE, _Arg = [])
	    end).

start_in_shell() ->
    {ok, Pid} = supervisor:start_link(?NAME, ?MODULE, _Arg = []),
    unlink(Pid).

start_link(Args) ->
    ?DEBUG("data server supervisor start_link []", []),
    supervisor:start_link(?NAME, ?MODULE, Args).

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

