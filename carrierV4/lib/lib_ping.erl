-module(lib_ping).
-export([ping/1]).

ping([Nodename]) ->
    net_adm:ping(Nodename),
    receive 
        after 200 ->
                true
        end.

