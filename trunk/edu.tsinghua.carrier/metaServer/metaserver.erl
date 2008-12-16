-module(metaserver).
-compile(export_all).

%-export([do_this_once/0, start_start_mnesia/0]).
-record(filemetaTable, {fileid, filename, filesize, chunklist, createT, modifyT, acl}).
-record(chunkmappingTalbe, {chunkid, chunklocations}).
%record current active client-metaserver sesseions
-record(clientinfo, {clientid, modes}).
-record(filesessionTable, {fileid, clientlist}).

do_this_once() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(filemeta, [{attributes, record_info(fields, filemetaTable)}]),
    mnesia:create_table(chunkmapping, [{attributes, record_info(fields, chunkmappingTalbe)}]),
    mnesia:stop().

start_mnesia()->
    mnesia:start(),
    mnesia:wait_for_tables([filemetaTable, chunkmappingTalbe], 20000).

%"model" methods
% write step 1: open file
do_open(Filename, Modes, ClientID)->
    % Modes 
    case Modes of
        r-> do_read_open(Filename, ClientID);
        w-> do_write_open(Filename, ClientID);
        a-> do_append_open(Filename, ClientID);
        _-> {error, "unkown open mode"}
    end.

do_read_open(Filename, ClientID)->
    % mock return
    {ok, <<16#ff00ff00ff00ff00:64>>}.

do_write_open(Filename, ClientID)->
    % mock return
    {ok, <<16#ff00ff00ff00ff00:64>>}.

do_append_open(Filename, ClientID)->
    % mock return
    {ok, <<16#ff00ff00ff00ff00:64>>}.

% write step 2: allocate chunk
do_allocate_chunk(FileID, ClientID)->
    % look up "filesession" for client open Modes
    Modes = look_up_filesession(FileID, ClientID),
    % allocate data chunk
    case Modes of
        w-> get_last_chunk(FileID);
        a-> get_last_chunk(FileID)
    end.
    
look_up_filesession(FileID, ClientID)->
    % mock return
    {w}.



% unused function  a
 get_first_chunk(FileID)->
    % mock return
    {ok, <<16#ff00ff00ff00ff00:64>>, [nodeip1, nodeip2, nodeip3]}.
    
get_last_chunk(FileID)->
    % mock return
    {ok, <<16#ff00ff00ff00ff00:64>>, [nodeip1, nodeip2, nodeip3]}.

% write step 3: register chunk
do_register_chunk(FileID, ChunkID, ChunkUsedSize, NodeList)->
    % register chunk
    % do nothing in Milestone ONE
    {ok, {}}.

% write step 4: close file
do_close(FileID, ClientID)->
    % delete client from filesession table
    {}.

% read step 1: open file == wirte step1
% read step 2: get chunk for further reading
do_get_chunk(FileID, ChunkIdx)->
    % mock return
{ok,  <<16#ff00ff00ff00ff00:64>>, [nodeip1, nodeip2, nodeip3]}.

% read step 4: close file == write step 4
