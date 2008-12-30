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
-include("filedevice.hrl").
-import(clientlib, 
	[do_open/2, do_pread/3, 
	do_pwrite/3, do_delete/1, 
	do_close/1, get_file_name/1]).
-export([open/2, pwrite/3, pread/3, delete/1, close/1]).
-compile(export_all).
%-define(BINARYSIZE, 67108864).
%-record(filedevice, {fileid, cursornodes, chunkid, cursoroffset}).
%% cursoroffset is offset in a chunk
%%----------------------------------------------------------------------
%% client_api,can be used for application programming
%%----------------------------------------------------------------------

open(FileName, Mode) ->
    do_open(FileName, Mode).

pwrite(FileDevice, Location, Bytes) ->
    do_pwrite(FileDevice, Location, Bytes).

pread(FileID, Start, Length) ->
    do_pread(FileID, Start, Length).

delete(FileName) ->
    do_delete(FileName).

close(FileID) ->
    do_close(FileID).


    
