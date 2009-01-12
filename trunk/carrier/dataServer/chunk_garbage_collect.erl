-module(chunk_garbage_collect).
-export([collect/1, start_auto_collect/0,stop_auto_collect/0]).
-import(lists, [foreach/2]).
-import(util,[for/3]).
-include("data_server.hrl").
-include("../include/egfs.hrl").

collect(GarbageList) ->
    chunk_db:insert_garbage_infos(GarbageList),
    RemoveFileList = chunk_db:get_paths_by_chunk_id(GarbageList),
    remove_files_on_disk(RemoveFileList),
    chunk_db:remove_garbage_infos(GarbageList),
    chunk_db:remove_chunk_infos(GarbageList).

remove_file_on_disk(FilePath) ->
     file:delete(FilePath).
        	  
remove_files_on_disk(FilePathList) ->
    foreach(fun remove_file_on_disk/1, FilePathList).
 
start_auto_collect() ->
    register(garbage_auto_collector, spawn_link(fun() -> loop(?GARBAGE_AUTO_COLLECT_PERIOD) end)).

stop_auto_collect() ->
    garbage_auto_collector ! stop.

loop(Time) ->
    receive
	stop ->
	    io:format("garbage auto collector stopped ~n")
    after Time -> 
	    io:format("garbage auto collector  event~n"),
	    {ok, HostProcName} = boot_report:get_proc_name(),
	    try gen_server:call(?META_SERVER, {getorphanchunk, HostProcName}) of
		 {ok, OrphanChunkList} ->
		    chunk_garbage_collect:collect(OrphanChunkList);
		_Any ->
		    io:format("garbage auto collector failed ~p ~n",[_Any])
	    catch
		throw:X ->
		    io:format("Thrown caught ~p ~n", [X]),
		    toolkit:sleep(?GARBAGE_AUTO_COLLECT_WAIT_TIME),
		    loop(Time);
		error:X ->
		    io:format("Error caught ~p ~n", [X]),
		    toolkit:sleep(?GARBAGE_AUTO_COLLECT_WAIT_TIME),
		    loop(Time);
		exit:X  ->
		    io:format("Exit caught ~p ~n", [X]),
		    toolkit:sleep(?GARBAGE_AUTO_COLLECT_WAIT_TIME),
		    loop(Time)
	    end,
	    loop(Time)
    end.
    


