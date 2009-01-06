-module(client_tests).
-export([ping/0,put_file/2]).
-include_lib("eunit/include/eunit.hrl").
-include("../include/egfs.hrl").
-include("../client/filedevice.hrl").
-import(lists, [map/2, sort/1]).
% -import(client, [open/2, close/1]).             

-define(BINARYSIZE, 67108864).


ping() ->
    net_adm:ping(zyb@zyb),
    receive 
        after 200 ->
                true
        end.

make_test_() ->
    {ok, F} = file:open("client_tests.log", write),
    {ok, SubFileDirs} = file:list_dir("."),
    SubFiles = lists:filter(fun(I) -> filelib:is_dir(I) =:= false end, SubFileDirs),
    map(fun(X) -> (fun() -> do_write(F,X) end) end, sort(SubFiles)). 

do_write(Log, Filename) ->
    statistics(runtime),
    statistics(wall_clock),
    RemoteFile = Filename,
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
    client:close(FileID#filedevice.fileid).

loop_write(Hdl, FileID, Start, Length) when Length > 0 ->
    {ok, Binary} = file:pread(Hdl, Start, ?BINARYSIZE),
    client:pwrite(FileID, Start ,Binary),
    Start1 = Start + ?BINARYSIZE,
    Length1 = Length - ?BINARYSIZE,
    loop_write(Hdl, FileID, Start1, Length1);

loop_write(_, _, _, _) ->
    ?DEBUG("[Client, ~p]:write file ok", [?LINE]).


%% do_get(Log, Filename) ->
%%     statistics(runtime),
%%     statistics(wall_clock),
%%     LocalFile = Filename,
%%     get_file(Filename, LocalFile),
%%     {_,Time1} = statistics(runtime),
%%     {_,Time2} = statistics(wall_clock),
%%     io:format(user, "[~-3s][RT:~10p][WT:~10p][size:~10p]:(W)~p~n", ["OK", Time1, Time2, filelib:file_size(Filename), Filename]),
%%     io:format(Log, "[~-3s][RT:~10p][WT:~10p][size:~10p]:(W)~p~n", ["OK", Time1, Time2, filelib:file_size(Filename), Filename]).

%% %% put(_Filename) ->
%% %%     true.

%% get_file(RemoteFile, LocalFile) ->
%%     ?DEBUG("[client, ~p]: test write begin at ~p~n ", [?LINE, erlang:time()]),
%%     io:format(user, "[client, ~p]: test write begin at ~p~n ", [?LINE, erlang:time()]),
%%     {ok, FileInfo} = client:read_file_info(RemoteFile),
%%     {FileLength, _, _, _, _} = FileInfo,
%%     {ok, RFile} = client:open(RemoteFile, r),	 %%only send open message to metaserver
%%     {ok, LFile} = file:open(LocalFile, [raw, write, binary]), 
%%     loop_read(RFile, LFile, 0, FileLength),
%%     ?DEBUG("[client, ~p]: test write end at ~p~n ", [?LINE, erlang:time()]),
%%     io:format(user, "[client, ~p]: test write end at ~p~n ", [?LINE, erlang:time()]),
%%     file:close(LFile),
%%     client:close(RFile).

%% loop_read(RFile, LFile, Start, Length) when Length > 0 ->
%%     {ok, Binary} = client:pread(RFile, Start, Length),
%%     {ok, _} = file:pwrite(LFile, Binary),
%%     Start1 = Start + ?BINARYSIZE,
%%     Length1 = Length - ?BINARYSIZE,
%%     loop_read(RFile, LFile, Start1, Length1);

%% loop_read(_, _, _, _) ->
%%     ?DEBUG("[Client, ~p]:read file ok", [?LINE]).

