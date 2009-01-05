%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : XCC & HXM
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(test).
-include("../include/egfs.hrl").
-include_lib("kernel/include/file.hrl").
-include("filedevice.hrl").
-import(clientlib,[get_file_name/1, get_file_size/1]).
-compile(export_all).
-define(BINARYSIZE, 67108864).%67108864  %8388608
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%              write function for write
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
write(FileName, RemoteFile) ->
    ?DEBUG("[client, ~p]:test write begin at ~p~n~n", [?LINE, erlang:time()]),
    {ok, FileLength} = get_file_size(FileName),
    {ok, FileDevice} = client:open(RemoteFile, w),	 %%only send open message to metaserver
    FileID = FileDevice#filedevice.fileid,
    {ok, Hdl} = get_file_handle(read, FileName),
    loop_write(Hdl, FileDevice, 0, FileLength),
    ?DEBUG("~n[client, ~p]:test write end at ~p~n", [?LINE, erlang:time()]),    
    file:close(Hdl),
    client:close(FileID).

loop_write(Hdl, FileDevice, Start, Length) when Length > 0 ->
    {ok, Binary} = read_tmp(Hdl, Start, ?BINARYSIZE),
    {ok, FileDevice1} = client:pwrite(FileDevice, Start ,Binary),
    Start1 = Start + ?BINARYSIZE,
    Length1 = Length - ?BINARYSIZE,
    loop_write(Hdl, FileDevice1, Start1, Length1);
loop_write(_, _, _, _) ->
    ?DEBUG("[Client, ~p]:write file ok~n", [?LINE]).
    
    
read_tmp(Hdl, Location, Size) ->
    file:pread(Hdl, Location, Size).

get_file_handle(write, FileName) ->
    file:open(FileName, [raw, append, binary]);
get_file_handle(read, FileName) ->
    file:open(FileName, [raw, read, binary]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%              read function for write
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
read(FileName, LocalFile, Start, Length) ->
    ?DEBUG("[client, ~p]:test read begin at ~p~n~n", [?LINE, erlang:time()]),
    {ok, FileDevice} = client:open(FileName, r),
    FileID = FileDevice#filedevice.fileid,
    client:pread(FileID, Start, Length),
    client:close(FileID),
    TempFileName = get_file_name(FileID),
    {ok, Hdl} = get_file_handle(read, TempFileName),
    {ok, FileSize} = get_file_size(TempFileName),
    {ok, DstHdl} = get_file_handle(write, LocalFile),
    loop_read_tmp(Hdl, DstHdl, 0, FileSize),
    file:close(DstHdl),
    file:delete(TempFileName),
    ?DEBUG("~n[client, ~p]:test read end at ~p~n", [?LINE, erlang:time()]).

loop_read_tmp(Hdl, DstHdl, Start, Length) when Length > 0 ->
    if
	Length - ?BINARYSIZE > 0 ->
	    Size = ?BINARYSIZE;
	true ->
	    Size = Length
    end,
    {ok, Binary} = read_tmp(Hdl, Start, Size),
    Length1 = Length - Size,
    Start1 = Start + Size,
    file:write(DstHdl, Binary),
    loop_read_tmp(Hdl, DstHdl, Start1, Length1);
loop_read_tmp(_, _, _, _) ->
    void.

    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%              read file from 0 to end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
read(FileName, LocalFile) ->
    ?DEBUG("[client, ~p]:test read begin at ~p~n~n", [?LINE, erlang:time()]),
    {ok, FileDevice} = client:open(FileName, r),
    FileID = FileDevice#filedevice.fileid,
    {ok, FileInfo} = client:read_file_info(FileName),
    {Length, _, _, _, _} = FileInfo,
    ?DEBUG("[client, ~p]:Length from read_file_info:~p~n~n", [?LINE, Length]),
    client:pread(FileID, 0, Length),
    client:close(FileID),
    TempFileName = get_file_name(FileID),
    {ok, Hdl} = get_file_handle(read, TempFileName),
    {ok, FileSize1} = get_file_size(TempFileName),
    {ok, DstHdl} = get_file_handle(write, LocalFile),
    loop_read_tmp(Hdl, DstHdl, 0, FileSize1),
    file:close(DstHdl),
    file:delete(TempFileName),
    ?DEBUG("~n[client, ~p]:test read end at ~p~n", [?LINE, erlang:time()]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%              delete function for write
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
del(FileName) ->
    client:delete(FileName),
    ?DEBUG("~n[client, ~p]:test read end at ~p~n", [?LINE, erlang:time()]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%              read infomation of file
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
read_info(FileName) ->
    {ok, FileInfo} = client:read_file_info(FileName),
    ?DEBUG("[client, ~p]:~n", [?LINE]),
    {FileSize, Chunklist, CreatT, ModifyT, Acl} = FileInfo,
    ?DEBUG("[client, ~p]:~n", [?LINE]),
    ?DEBUG("[client, ~p]:file  size: ~p~n", [?LINE, FileSize]),
    ?DEBUG("[client, ~p]:chunk list: ~p~n", [?LINE, Chunklist]),
    ?DEBUG("[client, ~p]:creat time: ~p~n", [?LINE, binary_to_term(CreatT)]),
    ?DEBUG("[client, ~p]:modifytime: ~p~n", [?LINE, binary_to_term(ModifyT)]),
    ?DEBUG("[client, ~p]:acl: ~p~n", [?LINE, Acl]).

