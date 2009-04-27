-module(ping_server).
-export([ping/0]).

-define(META_SERVER, ltms@lt).

ping() ->
    net_adm:ping(?META_SERVER),
    receive 
        after 200 ->
                true
        end.

