-module(client).
-import(server1,[rpc/2]).
-compile(export_all).


% write step 1: open file
open(Filename, Mode) ->
    % Modes
%Pid = spawn(fun() -> loop(Name, Mod, Mod:init()) end),
    case Mode of
        r-> read_open(Filename, Mode, Pid);
        w-> write_open(Filename, Mode, Pid);
        a-> append_open(Filename, Mode, Pid);
        _-> {error, "unkown open mode"}
    end.


read_open(Filename, Mode, Pid)->
    % mock return
    [{ok,FileID}|{error,reason}].

write_open(Filename, Mode, Pid)->
    % mock return
    [{ok,FileID}|{error,reason}].

append_open(Filename, Mode, Pid)->
    % mock return
    [{ok,FileID}|{error,reason}].

write(Filename)->
    %add open file code here
    [{ok,FileID}|{error,reason}].

read(Filename,File_start,File_end)->
    %add open file code here
    [{ok,FileID}|{error,reason}].



% write step 2: allocate chunk
allocate_chunk(FileID, Pid)->
    % look up "filesession" for client open Modes
    [{ok, ChunkID,NodeList}|{error,reason}].


% write step 3: write chunk
write_chunk(FileID, FileID, ChunkID, NodeList, Pid)->
    % look up "filesession" for client open Modes
    [{ok, state}|{error,reason}].

% write/read step 4: close file
close(FileID, Pid)->
    % delete client from filesession table
    {}.


% read step 2: locate chunk
locate_chunk(FileID, Pid, ChunkIndex)->
    % look up "filesession" for client open Modes
    [{ok, ChunkID,NodeList}|{error,reason}].


% read step 3: read chunk
read_chunk(FileID, ChunkID, NodeList, Byterange, Pid)->
    % look up "filesession" for client open Modes
    [{ok, state}|{error,reason}].