-module(client_tests).
-export([ping/0,put_file/2, get_file/2]).
-include_lib("eunit/include/eunit.hrl").
-include("../include/egfs.hrl").
-include("../client/filedevice.hrl").
-import(lists, [map/2, sort/1]).
%% -import(client, [open/2, close/1]).             

-define(BINARYSIZE, 67108864).


ping() ->
    net_adm:ping(zyb@llk),
    receive 
        after 200 ->
                true
        end.

make_test_() ->
    {ok, F} = file:open("client_tests.log", write),
    {ok, SubFileDirs} = file:list_dir("./ds"),
    SubFileDirsPath = map(fun(X) -> string:concat("./ds/", X) end, SubFileDirs),
    SrcFiles = sort(lists:filter(fun(I) -> filelib:is_dir(I) =:= false end, SubFileDirsPath)),
    DstFiles = lists:map(fun(Src) -> 
                                 {ok, Dst, _} = regexp:sub(Src, "/ds/", "/re/"),
                                 {Src, Dst}
                         end, SrcFiles),
    %% [map(fun(X) -> {X,{timeout, 10000, fun() -> do_write(F,X) end}} end, SrcFiles),
    %%  map(fun(X) ->
    %%              {Src, Dst} = X,
    %%              {Dst,{timeout, 10000, fun() -> do_get(F,Src, Dst) end}} end, DstFiles)]. 

    %% [map(fun(X) -> {X,{timeout, 10000, fun() -> do_write(F,X) end}} end, SrcFiles)].
    [map(fun(X) ->
                 {Src, Dst} = X,
                 {Dst,{timeout, 10, fun() -> do_get(F,Src, Dst) end}} end, DstFiles)]. 


%% do_write(_Log, Filename) ->
%%     io:format(user, "[G:~p]: [F:~p]~n", [erlang:time(), Filename]).

do_write(Log, Filename) ->
    statistics(runtime),
    statistics(wall_clock),
    RemoteFile = Filename,
    put_file(Filename, RemoteFile),
    {_,Time1} = statistics(runtime),
    {_,Time2} = statistics(wall_clock),
    %% io:format(user, "[~-3s][RT:~10p][WT:~10p][size:~10p]:(W)~p~n", ["OK", Time1, Time2, filelib:file_size(Filename), Filename]),
    io:format(Log, "[~-3s][RT:~10p][WT:~10p][size:~10p]:(W)~p~n", ["OK", Time1, Time2, filelib:file_size(Filename), Filename]).

%% put(_Filename) ->
%%     true.

put_file(FileName, RemoteFile) ->
    io:format(user, "[P:~p]: [F:~p]~n", [erlang:time(), FileName]),
    FileLength = filelib:file_size(FileName),
    {ok, FileID} = client:open(RemoteFile, w),	 %%only send open message to metaserver
    {ok, Hdl} = file:open(FileName, [raw, read, binary]), 
    loop_write(Hdl, FileID, 0, FileLength),
    %% io:format(user, "[client, ~p]: test write end at ~p~n ", [?LINE, erlang:time()]),
    file:close(Hdl),
    client:close(FileID#filedevice.fileid).

loop_write(Hdl, FileID, Start, Length) when Length > 0 ->
    {ok, Binary} = file:pread(Hdl, Start, ?BINARYSIZE),
    client:pwrite(FileID, Start ,Binary),
    Start1 = Start + size(Binary),
    Length1 = Length - size(Binary),
    loop_write(Hdl, FileID, Start1, Length1);

loop_write(_, _, _, _) ->
    ?DEBUG("[Client, ~p]:write file ok", [?LINE]).

%% do_get(_Log, Src, Dst) ->
%%     io:format(user, "[P:~p]: [Carrier:~p => Local:~p]~n", [erlang:time(), Src, Dst]).


do_get(Log, RemoteFile, LocalFile) ->
    statistics(runtime),
    statistics(wall_clock),
    get_file(RemoteFile, LocalFile),
    {_,Time1} = statistics(runtime),
    {_,Time2} = statistics(wall_clock),
    %% io:format(user, "[~-3s][RT:~10p][WT:~10p][size:~10p]:(W)~p~n", ["OK", Time1, Time2, filelib:file_size(Filename), Filename]),
    io:format(Log, "[~-3s][RT:~10p][WT:~10p][size:~10p]:(W)~p~n", ["OK", Time1, Time2, filelib:file_size(LocalFile), LocalFile]).

%% get_file(RemoteFile, LocalFile) ->
%%     io:format(user, "[P:~p]: [Carrier:~p => Local:~p]~n", [erlang:time(), RemoteFile, LocalFile]).

get_file(RemoteFile, LocalFile) ->
    io:format(user, "[P:~p]: [Carrier:~p => Local:~p]~n", [erlang:time(), RemoteFile, LocalFile]),
    {ok, FileInfo} = client:read_file_info(RemoteFile),
    {FileLength, _, _, _, _} = FileInfo,
    {ok, RFile} = client:open(RemoteFile, r),	 %%only send open message to metaserver
    RFileID = RFile#filedevice.fileid,
    {ok, LFile} = file:open(LocalFile, [raw, write, binary]), 
    loop_read(RFileID, LFile, 0, FileLength),
    file:close(LFile),
    client:close(RFile).

loop_read(RFile, LFile, Start, Length) when Length > 0 ->
    %% case client:pread(RFile, Start, ?BINARYSIZE) of 
    %%     {ok, Binary} -> 
    %%         {ok, _} = file:pwrite(LFile, Binary),
    %%         Start1 = Start + ?BINARYSIZE,
    %%         Length1 = Length - ?BINARYSIZE,
    %%         loop_read(RFile, LFile, Start1, Length1);
    %%     {eof} ->
    %%         true;
    %%     Any ->
    %%         Any
    %% end
    {ok, Binary} = client:pread(RFile, Start, ?BINARYSIZE),
    {ok, _} = file:pwrite(LFile, Binary),
    Start1 = Start + size(Binary),
    Length1 = Length - size(Binary),
    loop_read(RFile, LFile, Start1, Length1);

loop_read(_, _, _, _) ->
    true.


