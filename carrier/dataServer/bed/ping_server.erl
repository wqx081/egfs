-module(ping_server).
-export([ping/0]).

ping() ->
    net_adm:ping(zyb@llk),
    receive 
        after 200 ->
                true
        end.

