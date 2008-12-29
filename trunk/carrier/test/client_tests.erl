-module(client_tests).
-export([ping/0, fib/1]).
-include_lib("eunit/include/eunit.hrl").
% -import(client, [write/1, read/2]).

ping() ->
    net_adm:ping(zyb@zyb),
    receive 
        after 200 ->
                true
        end.

%% write_test() ->
%%     ?_assert(1+1==2).

fib(0) -> 1;
fib(1) -> 1;
fib(N) when N > 1 -> fib(N-1) + fib(N-2).

client_test_() ->
    [?_assert(fib(0) == 1),
     ?_assert(fib(1) == 1),
     ?_assert(fib(2) == 2),
     ?_assert(fib(3) == 3),
     ?_assert(fib(4) == 5),
     ?_assert(fib(5) == 8),
     ?_assertException(error, function_clause, fib(-1)),
     ?_assert(fib(31) == 2178309)
    ].

%% test(Filename) ->
%%     statistics(runtime),
%%     statistics(wall_clock),
%%     Result = try write(Filename) of
%%         _ -> "OK"
%%     catch
%%         _:_ -> "ERR"
%%     end,
%%     {_,Time1} = statistics(runtime),
%%     {_,Time2} = statistics(wall_clock),
%%     io:format("[~-3s][RT:~10p][WT:~10p]:(W)~p~n", [Result, Time1, Time2, Filename]).

