-module(t_client).
-export([test/1, ping/0]).
-import(client, [write/1, read/2]).

ping() ->
    net_adm:ping(zyb@zyb),
    receive 
        after 200 ->
                true
        end.

test(Filename) ->
    statistics(runtime),
    statistics(wall_clock),
    Result = try write(Filename) of
        _ -> "OK"
    catch
        _:_ -> "ERR"
    end,
    {_,Time1} = statistics(runtime),
    {_,Time2} = statistics(wall_clock),
    io:format("[~-3s][RT:~10p][WT:~10p]:(W)~p~n", [Result, Time1, Time2, Filename]).

