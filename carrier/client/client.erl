%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : XCC & HXM
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(client).
-include("../include/egfs.hrl").
-include_lib("kernel/include/file.hrl").
-import(clientlib, 
	[do_open/2, do_pread/3, 
	write_them/3, do_delete/1, 
	do_close/1, get_file_name/1]).
-export([test_w/2,open/2, pwrite/3, pread/3, delete/1, close/1]).
-compile(export_all).
-define(BINARYSIZE, 67108864).

%%----------------------------------------------------------------------
%% client_api,can be used for application programming
%%----------------------------------------------------------------------

open(FileName, Mode) ->
    do_open(FileName, Mode).

pwrite(FileID, Location, Bytes) ->
    write_them(FileID, Location, Bytes).
    %%do_pwrite(FileID, Location, Bytes).

pread(FileID, Start, Length) ->
    do_pread(FileID, Start, Length).

delete(FileName) ->
    do_delete(FileName).

close(FileID) ->
    do_close(FileID).

test_w(FileName, RemoteFile) ->
    {ok, FileLength} = get_file_size(FileName),
    FileID = open(RemoteFile, w),
    {ok, Hdl} = get_file_handle(FileName),
    loop_write(Hdl, FileID, 0, FileLength),
    close(FileID).

loop_write(Hdl, FileID, Start, Length) when Length > 0 ->
    Binary = read_tmp(Hdl, Start, ?BINARYSIZE),
    pwrite(FileID, Start ,Binary),
    Start1 = Start + ?BINARYSIZE,
    Length1 = Length - ?BINARYSIZE,
    loop_write(Hdl, FileID, Start1, Length1);
loop_write(_, _, _, _) ->
    ?DEBUG("[Client, ~p]:write file ok", [?LINE]).
    
    
read_tmp(Hdl, Location, Size) ->
    case  file:pread(Hdl, Location, Size) of
	{ok, Binary} ->
	    Binary;
	{error, Why} ->
	    ?DEBUG("read file(~p, ~p) error: ~p, ~n",[Location, Size, Why]),
	    []
    end.

get_file_handle(FileName) ->
    case file:open(FileName, [raw, append, binary]) of
	{ok, Hdl} ->	
	    {ok, Hdl};
	{error, Why} ->
	    ?DEBUG("[Client, ~p]:Open file error:~p", [?LINE, Why]),
	    void
    end.

get_file_size(FileName) ->
    case file:read_file_info(FileName) of
	{ok, Facts} ->
	    {ok, Facts#file_info.size};
	_ ->
	    error
    end.

test_r(FileName, LocalFile, Start, Length) ->
    FileID = open(FileName, r),
    pread(FileID, Start, Length),
    close(FileID),
    TempFileName = get_file_name(FileID),
    {ok, Hdl} = get_file_handle(TempFileName),
    FileSize = get_file_size(TempFileName),
    loop_read_tmp(Hdl, LocalFile, 0, FileSize).

loop_read_tmp(Hdl, LocalFile, Start, Length) when Length > 0 ->
    if
	Length - ?BINARYSIZE > 0 ->
	    Size = ?BINARYSIZE;
	true ->
	    Size = Length
    end,
    Binary = read_tmp(Hdl, Start, Size),
    Length1 = Length - ?BINARYSIZE,
    Start1 = Start + ?BINARYSIZE,
    file:write_file(LocalFile, Binary),
    loop_read_tmp(Hdl, LocalFile, Start1, Length1);
loop_read_tmp(_, _, _, _) ->
    void.
    
