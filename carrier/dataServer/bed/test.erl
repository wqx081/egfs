-module(test).
-export([run/0]).
-define(SERVER, {data_server, lt@lt}).

-define(CHKID, <<0,0,172,10,0,9,103,237>>).

run() ->
    spawn_link(fun() -> do_it() end),
    io:format("second~n"),
    spawn(fun() -> do_it() end),
    ok.


do_it() ->
    Name = get_pname(read, ?CHKID, self()),
    true = register(Name, self()),
    io:format("~p~n", [Name]),
    Result1 = gen_server:call(?SERVER, {echo, {node(), self()}}),
    io:format("~p~n", [Result1]),
    Result2 = gen_server:call(?SERVER, {echo, {node(), self()}}),
    io:format("~p~n", [Result2]).

get_pname(read, ChunkID, CPid) ->
    Bin = term_to_binary({read, ChunkID, CPid}),
    List = binary_to_list(Bin),
    _Atom = list_to_atom(List).

