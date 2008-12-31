-module(client_tests).
-export([ping/0]).
-include_lib("eunit/include/eunit.hrl").
-include("../include/egfs.hrl").
-import(lists, [map/2, sort/1]).
% -import(client, [open/2, close/1]).             

-define(BINARYSIZE, 67108864).


ping() ->
    net_adm:ping(zyb@zyb),
    receive 
        after 200 ->
                true
        end.

%% write_test() ->
%%     ?_assert(1+1==2).

%% fib(0) -> 1;
%% fib(1) -> 1;
%% fib(N) when N > 1 -> fib(N-1) + fib(N-2).

%% client_test_() ->
%%     [?_assert(fib(0) == 1),
%%      ?_assert(fib(1) == 1),
%%      ?_assert(fib(2) == 2),
%%      ?_assert(fib(3) == 3),
%%      ?_assert(fib(4) == 5),
%%      ?_assert(fib(5) == 8),
%%      ?_assertException(error, function_clause, fib(-1)),
%%      ?_assert(fib(31) == 2178309)
%%     ].

write_test_() ->
    {ok, F} = file:open("client_tests.log", write),
    {ok, SubFileDirs} = file:list_dir("."),
    SubFiles = lists:filter(fun(I) -> filelib:is_dir(I) =:= false end, SubFileDirs),
    map(fun(X) -> (fun() -> do_write(F,X) end) end, sort(SubFiles)). 

do_write(Log, Filename) ->
    statistics(runtime),
    statistics(wall_clock),
    RemoteFile = Filename,
    %% Result = try put_file(Filename, RemoteFile) of
    %%              {ok, _} -> "OK";
    %%              _Any -> "ERR"
    %%          catch
    %%              _:_ -> "ERR"
    %%          end,
    put_file(Filename, RemoteFile),
    {_,Time1} = statistics(runtime),
    {_,Time2} = statistics(wall_clock),
    io:format(user, "[~-3s][RT:~10p][WT:~10p][size:~10p]:(W)~p~n", ["OK", Time1, Time2, filelib:file_size(Filename), Filename]),
    io:format(Log, "[~-3s][RT:~10p][WT:~10p][size:~10p]:(W)~p~n", ["OK", Time1, Time2, filelib:file_size(Filename), Filename]).

%% put(_Filename) ->
%%     true.

put_file(FileName, RemoteFile) ->
    ?DEBUG("[client, ~p]: test write begin at ~p~n ", [?LINE, erlang:time()]),
    io:format(user, "[client, ~p]: test write begin at ~p~n ", [?LINE, erlang:time()]),
    FileLength = filelib:file_size(FileName),
    {ok, FileID} = client:open(RemoteFile, w),	 %%only send open message to metaserver
    {ok, Hdl} = file:open(FileName, [raw, read, binary]), 
    loop_write(Hdl, FileID, 0, FileLength),
    ?DEBUG("[client, ~p]: test write end at ~p~n ", [?LINE, erlang:time()]),
    io:format(user, "[client, ~p]: test write end at ~p~n ", [?LINE, erlang:time()]),
    file:close(Hdl),
    client:close(FileID)
.

loop_write(Hdl, FileID, Start, Length) when Length > 0 ->
    {ok, Binary} = file:pread(Hdl, Start, ?BINARYSIZE),
    client:pwrite(FileID, Start ,Binary),
    Start1 = Start + ?BINARYSIZE,
    Length1 = Length - ?BINARYSIZE,
    loop_write(Hdl, FileID, Start1, Length1);

loop_write(_, _, _, _) ->
    ?DEBUG("[Client, ~p]:write file ok", [?LINE]).
