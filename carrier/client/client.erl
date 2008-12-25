%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : XCC & HXM
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  
%%%-------------------------------------------------------------------
-module(client).
-include("../include/egfs.hrl").
-import(clientlib, [do_open/2, do_pread/3, do_pwrite/3, do_delete/1, do_close/1]).
-export([test_w/2,open/2, pwrite/3, pread/3, delete/1, close/1]).
-compile(export_all).

%%----------------------------------------------------------------------
%% client_api,can be used for application programming
%%----------------------------------------------------------------------

open(FileName, Mode) ->
    do_open(FileName, Mode).

pwrite(FileID, Location, Bytes) ->
    do_pwrite(FileID, Location, Bytes).

pread(FileID, Start, Length) ->
    do_pread(FileID, Start, Length),
    case read_tmp("/tmp/temp.txt") of
	Binary ->
	    Binary
    end.

delete(FileName) ->
    do_delete(FileName).

close(FileID) ->
    do_close(FileID).

test_w(FileName, RemoteFile) ->
    %{ok,Hdl} = file:open(FileName, [binary, raw, read, read_ahead]),
    %{ok, Binary} = file:read_file(FileName)
    %file:close(Hdl),
    Binary =read_tmp(FileName),
    FileID =open(RemoteFile,w),
    pwrite(FileID,0,Binary),
    close(FileID).

read_tmp(FileName) ->
    case  file:read_file(FileName) of
	{ok, Binary} ->
	    Binary;
	{error, Why} ->
	    ?DEBUG("read file error: ~p~n",[Why]),
	    []
    end.

test_r(FileName, LocalFile, Start, Length) ->
    FileID =open(FileName, r),
    Binary =pread(FileID, Start, Length),
    close(FileID),
    {ok, Hdl} = file:open(LocalFile, [raw, append, binary]),
    file:write(Hdl, Binary),
    file:close(Hdl).
