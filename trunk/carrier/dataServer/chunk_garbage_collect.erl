-module(chunk_garbage_collect).
-compile(export_all).

-import(lists, [foreach/2]).
-import(util,[for/3]).


-include("garbage_info.hrl").
-include("../include/egfs.hrl").

-define(GM,{global, metagenserver}).

collect(GarbageList) ->
    %io:format("[~p, ~p] ~n", [?MODULE, ?LINE]),
    chunk_db:insert_garbage_infos(GarbageList),
    %io:format("[~p, ~p] ~n", [?MODULE, ?LINE]),    
    RemoveFileList = chunk_db:get_paths_by_chunk_id(GarbageList),
    %io:format("[~p, ~p] ~n", [?MODULE, ?LINE]),
    remove_files_on_disk(RemoveFileList),
    %io:format("[~p, ~p] ~n", [?MODULE, ?LINE]),
    chunk_db:remove_garbage_infos(GarbageList),
    %io:format("[~p, ~p] ~n", [?MODULE, ?LINE]),
    chunk_db:remove_chunk_infos(GarbageList),
    io:format("[~p, ~p] ~n", [?MODULE, ?LINE]).

remove_file_on_disk(FilePath) ->
     file:delete(FilePath).

        	  
remove_files_on_disk(FilePathList) ->
    foreach(fun remove_file_on_disk/1, FilePathList).
    


