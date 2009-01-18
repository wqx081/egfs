-module(chunk_db).
-import(lists, [foreach/2]).
-import(util,[for/3]).

-include("data_server.hrl").
-include("../include/egfs.hrl").
-include_lib("stdlib/include/qlc.hrl").

-compile(export_all).

do_this_once() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(chunkmeta, [{attributes, record_info(fields, chunkmeta)}, {disc_copies,[node()]}]),
    mnesia:create_table(garbageinfo, [{attributes, record_info(fields, garbageinfo)}, {disc_copies,[node()]}]),
    mnesia:stop().

start()->
    case mnesia:create_schema([node()]) of
	ok ->
	    mnesia:start(),
	    mnesia:create_table(chunkmeta, [{attributes, record_info(fields, chunkmeta)}, {disc_copies,[node()]}]),
	    mnesia:create_table(garbageinfo, [{attributes, record_info(fields, garbageinfo)}, {disc_copies,[node()]}]),
	    mnesia:wait_for_tables([chunkmeta, garbageinfo], 5000);
	_ -> 
	    mnesia:start(),
	    mnesia:wait_for_tables([chunkmeta, garbageinfo], 5000)
    end.

    

stop()->
    mnesia:stop().

clear_tables()->
    mnesia:clear_table(chunkmeta),
    mnesia:clear_table(garbageinfo).
    

do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Val} = mnesia:transaction(F),
    Val.

do_trans(X) ->
    F = fun() ->
	    mnesia:write(X)
    end,
    {atomic, Val} = mnesia:transaction(F),
    Val.



insert_chunk_info(Chunkid, Fileid, Path, Size) ->
    Row = #chunkmeta{file_id=Fileid,
		     chunk_id=Chunkid, 
		     path=Path,
		     length=Size,
		     create_time=erlang:localtime(), 
		     modify_time=(erlang:localtime())
		    },
    do_trans(Row).





remove_chunk_info(Chunkid)->
    Oid = {chunkmeta, Chunkid},
    F = fun() ->
		mnesia:delete(Oid)
	end,
    mnesia:transaction(F).

remove_chunk_infos(ChunkidList) ->
    F = fun() ->
		foreach(fun remove_chunk_info/1, ChunkidList)
	end,
    mnesia:transaction(F).
    



modify_chunk_info(Chunkid, NewPath, NewSize, NewCreatetime, NewModifytime) ->
    [{chunkmeta,Chunkid, Fileid, _, _, _, _ }] = get_chunk_info_by_id(Chunkid),    
    Row = #chunkmeta{chunk_id=Chunkid,
		     file_id=Fileid,
		     path=NewPath,
		     length=NewSize,
		     create_time=NewCreatetime,
		     modify_time=NewModifytime      
		    },
    do_trans(Row).

modify_chunk_path(Chunkid, NewPath) ->
    [{chunkmeta, Chunkid, Fileid, _, Size, Createtime, Modifytime}] = get_chunk_info_by_id(Chunkid),    
     Row = #chunkmeta{chunk_id=Chunkid,
		      file_id=Fileid,
		      path=NewPath,
		      length=Size,
		      create_time=Createtime,
		      modify_time=Modifytime      
 		    },
    do_trans(Row).


modify_chunk_length(Chunkid, NewSize) ->
    [{chunkmeta, Chunkid, Fileid, Path, _, Createtime, Modifytime}] = get_chunk_info_by_id(Chunkid),    
     Row = #chunkmeta{chunk_id=Chunkid,
		      file_id=Fileid,
		      path=Path,
		      length=NewSize,
		      create_time=Createtime,
		      modify_time=Modifytime      
 		    },

    do_trans(Row).

modify_chunk_ct(Chunkid, NewCreatetime) ->
    [{chunkmeta, Chunkid, Fileid, Path, Size, _, Modifytime}] = get_chunk_info_by_id(Chunkid),    
     Row = #chunkmeta{chunk_id=Chunkid,
		      file_id=Fileid,
		      path=Path,
		      length=Size,
		      create_time=NewCreatetime,
		      modify_time=Modifytime      
 		    },
    do_trans(Row). 
    

modify_chunk_mt(Chunkid, NewModifytime) ->
    [{chunkmeta, Chunkid, Fileid, Path, Size, Createtime, _}] = get_chunk_info_by_id(Chunkid),    
     Row = #chunkmeta{chunk_id=Chunkid,
		      file_id=Fileid,
		      path=Path,
		      length=Size,
		      create_time=Createtime,
		      modify_time=NewModifytime      
		     },
    do_trans(Row).


get_all_from_table(T)->
    do(qlc:q([X || X <- mnesia:table(T)])).

get_chunk_info_by_id(Chunkid) ->  
    do(qlc:q([X || X <- mnesia:table(chunkmeta), 
		   X#chunkmeta.chunk_id =:= Chunkid
		      ])).

get_chunk_id_by_path(Path) ->
    do(qlc:q([X#chunkmeta.chunk_id || X <- mnesia:table(chunkmeta),
                                   X#chunkmeta.path =:= Path
					 ])).   

get_file_id_by_chunk_id(Chunkid) ->
    do(qlc:q([X#chunkmeta.file_id || X <- mnesia:table(chunkmeta), 
				     X#chunkmeta.chunk_id =:= Chunkid
					])).

get_all_chunk_id() ->
    do(qlc:q([X#chunkmeta.chunk_id || X <- mnesia:table(chunkmeta)				     
					 ])).
get_path_by_chunk_id(Chunkid) ->
    do(qlc:q([X#chunkmeta.path || X <- mnesia:table(chunkmeta), 
				     X#chunkmeta.chunk_id =:= Chunkid
					])).

get_paths_by_chunk_id(ChunkidList) ->
    TmpChunkidList = [get_path_by_chunk_id(X) || X <- ChunkidList],
    [X || [X] <- TmpChunkidList].



insert_garbage_info(Chunkid) ->
    Row = #garbageinfo{chunk_id=Chunkid,
		      insert_time=erlang:localtime()
		      },
    do_trans(Row).

insert_garbage_infos(ChunkidList) ->
    F = fun() ->
  		foreach(fun insert_garbage_info/1, ChunkidList)
  	end,
    mnesia:transaction(F).


remove_garbage_info(Chunkid) ->
    Oid = {garbageinfo, Chunkid},
    F = fun() ->
		mnesia:delete(Oid)
	end,
    mnesia:transaction(F).

    
remove_garbage_infos(ChunkidList) ->
    F = fun() ->
		foreach(fun remove_garbage_info/1, ChunkidList)
	end,
    mnesia:transaction(F).


get_all_garbage_chunk_id() ->
    do(qlc:q([X#garbageinfo.chunk_id || X <- mnesia:table(garbageinfo)
				       ])).

    
