-module(ping_server).
-export([ping/0]).
-import(client, [write/1, read/2]).

ping() ->
    net_adm:ping(zyb@zyb),
    receive 
        after 200 ->
                true
        end.

