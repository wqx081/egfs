-module(chunk_garbage_collect).
-export([collect/1, start_auto_collect/0,stop_auto_collect/0]).
-import(lists, [foreach/2]).
-import(util,[for/3]).
-include("data_server.hrl").
-include("../include/egfs.hrl").

collect(GarbageList)               ->
    chunk_db:insert_garbage_infos(GarbageList),
    RemoveFileList = chunk_db:get_paths_by_chunk_id(GarbageList),
    remove_files_on_disk(RemoveFileList),
    chunk_db:remove_garbage_infos(GarbageList),
    chunk_db:remove_chunk_infos(GarbageList).

remove_files_on_disk(FilePathList) ->
    foreach(fun file:delete/1, FilePathList).

start_auto_collect()               ->
    register(garbage_auto_collector, spawn_link(fun() -> loop(?GARBAGE_AUTO_COLLECT_PERIOD) end)).

stop_auto_collect()                ->
    garbage_auto_collector ! stop.

loop(Time) ->
    receive
	stop ->
	    void
    after Time -> 
	    {ok, HostProcName} = boot_report:get_proc_name(),
	    try gen_server:call(?META_SERVER, {getorphanchunk, HostProcName}) of
		{ok, OrphanChunkList} ->
		    chunk_garbage_collect:collect(OrphanChunkList);
		_Any ->
		    void
	    catch
		exit:X ->
		    toolkit:sleep(?GARBAGE_AUTO_COLLECT_WAIT_TIME),
		    loop(Time);
		  _Any ->
		    void
	    end,
	    loop(Time)
    end.



