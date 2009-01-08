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
	do_close/1, do_read_file/1,
	do_read_file_info/1]).
-export([open/2, pwrite/3, pread/3, read_file/1,
	read_file_info/1, delete/1, close/1]).
-compile(export_all).
%-define(BINARYSIZE, 67108864).
%-record(filedevice, {fileid, cursornodes, chunkid, cursoroffset}).
%% cursoroffset is offset in a chunk
%%----------------------------------------------------------------------
%% client_api,can be used for application programming
%%----------------------------------------------------------------------


%% --------------------------------------------------------------------
%% Function: open/2
%% Description: call metaserver to open file
%% Returns: {ok, Filedevice}          |
%%          {error, Why}              |
%% --------------------------------------------------------------------
open(FileName, Mode) ->
    do_open(FileName, Mode).

%% --------------------------------------------------------------------
%% Function: pwrite/3
%% Description: write Bytes to dataserver
%% Returns: {ok, Filedevice}          |
%%          {error, Why}              |
%% --------------------------------------------------------------------
pwrite(FileDevice, Location, Bytes) ->
    do_pwrite(FileDevice, Location, Bytes).

%% --------------------------------------------------------------------
%% Function: pread/3
%% Description: read Bytes from dataserver
%% Returns: {ok, Binary}	      |
%%          {error, Why}              |
%% --------------------------------------------------------------------
pread(FileDevice, Start, Length) ->
    do_pread(FileDevice, Start, Length).

%% --------------------------------------------------------------------
%% Function: read_file/1
%% Description: read a file from dataserver
%% Returns: {ok, Binary}	      |
%%          {error, Why}	      |
%% --------------------------------------------------------------------
read_file(FileName) ->
    do_read_file(FileName).

%% --------------------------------------------------------------------
%% Function: read_file_info/1
%% Description: read a file info from metaserver
%% Returns: {ok, FileInfo}	      |
%%          {error, Why}	      |
%% --------------------------------------------------------------------
read_file_info(FileName) ->
    do_read_file_info(FileName).

%% --------------------------------------------------------------------
%% Function: del_file/3
%% Description: delete file which at dataserver
%% Returns: ok			      |
%%          {error, Why}	      |
%% --------------------------------------------------------------------
delete(FileName) ->
    do_delete(FileName).

%% --------------------------------------------------------------------
%% Function: close/1
%% Description: delete file which at dataserver
%% Returns: ok			      |
%%          {error, Why}	      |
%% --------------------------------------------------------------------
close(FileDevice) ->
    do_close(FileDevice).


    
