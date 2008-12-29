%% Author: pyy
%% Created: 2008-12-25


-module(chunk_meta_db).
-import(lists, [foreach/2]).
-import(util,[for/3]).

%% Include files

-include("chunk_meta_format.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% Exported Functions
-compile(export_all).


%% API Functions


do_this_once() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(chunkmeta, [{attributes, record_info(fields, chunkmeta)}, {disc_copies,[node()]}]),
    mnesia:create_table(hostinfo, [{attributes, record_info(fields, hostinfo)}, {disc_copies,[node()]}]),
    mnesia:stop().

start()->
    mnesia:start(),
    mnesia:wait_for_tables([chunkmeta], 30000).

stop()->
    mnesia:stop().

clear_tables()->
    mnesia:clear_table(chunkmeta),
    mnesia:clear_table(hostinfo).



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


%% Insert one metadata into database.
insert_meta(Chunkid, Fileid, Path, Size) ->
    Row = #chunkmeta{file_id=Fileid,
		     chunk_id=Chunkid, 
		     path=Path,
		     size=Size,
		     create_time=erlang:localtime(), 
		     modify_time=(erlang:localtime())
		    },
    do_trans(Row).




%% remove one metadata from database where chunk_id equals Chunkid
remove_meta(Chunkid)->
    Oid = {chunkmeta, Chunkid},
    F = fun() ->
		mnesia:delete(Oid)
	end,
    mnesia:transaction(F).

    
%% modify the path,size,create_time and modifytime of one metadata 
%% where chunk_id equals Chunkid
modify_meta(Chunkid, NewPath, NewSize, NewCreatetime, NewModifytime) ->
    [{chunkmeta,Chunkid, Fileid, _, _, _, _ }] = get_meta_by_id(Chunkid),    
    Row = #chunkmeta{chunk_id=Chunkid,
		     file_id=Fileid,
		     path=NewPath,
		     size=NewSize,
		     create_time=NewCreatetime,
		     modify_time=NewModifytime      
		    },
    do_trans(Row).

modify_path(Chunkid, NewPath) ->
    [{chunkmeta, Chunkid, Fileid, _, Size, Createtime, Modifytime}] = get_meta_by_id(Chunkid),    
     Row = #chunkmeta{chunk_id=Chunkid,
		      file_id=Fileid,
		      path=NewPath,
		      size=Size,
		      create_time=Createtime,
		      modify_time=Modifytime      
 		    },
    do_trans(Row).


modify_size(Chunkid, NewSize) ->
    [{chunkmeta, Chunkid, Fileid, Path, _, Createtime, Modifytime}] = get_meta_by_id(Chunkid),    
     Row = #chunkmeta{chunk_id=Chunkid,
		      file_id=Fileid,
		      path=Path,
		      size=NewSize,
		      create_time=Createtime,
		      modify_time=Modifytime      
 		    },

    do_trans(Row).

modify_ct(Chunkid, NewCreatetime) ->
    [{chunkmeta, Chunkid, Fileid, Path, Size, _, Modifytime}] = get_meta_by_id(Chunkid),    
     Row = #chunkmeta{chunk_id=Chunkid,
		      file_id=Fileid,
		      path=Path,
		      size=Size,
		      create_time=NewCreatetime,
		      modify_time=Modifytime      
 		    },
    do_trans(Row). 
    

modify_mt(Chunkid, NewModifytime) ->
    [{chunkmeta, Chunkid, Fileid, Path, Size, Createtime, _}] = get_meta_by_id(Chunkid),    
     Row = #chunkmeta{chunk_id=Chunkid,
		      file_id=Fileid,
		      path=Path,
		      size=Size,
		      create_time=Createtime,
		      modify_time=NewModifytime      
		     },

    do_trans(Row).

%% select function

%% select * from T
get_all_from(T)->
    do(qlc:q([X || X <- mnesia:table(T)])).

%% select * from  'chunkmeta' where 'chunk_id' = Chunkid
get_meta_by_id(Chunkid) ->  
    do(qlc:q([X || X <- mnesia:table(chunkmeta), 
		   X#chunkmeta.chunk_id =:= Chunkid
		      ])).


%% select 'chunk_id' from 'chunkmeta' where 'path' = Path
get_id_by_path(Path) ->
    do(qlc:q([X#chunkmeta.chunk_id || X <- mnesia:table(chunkmeta),
                                   X#chunkmeta.path =:= Path
					 ])).   
get_fileid_by_id(Chunkid) ->
    do(qlc:q([X#chunkmeta.file_id || X <- mnesia:table(chunkmeta), 
				     X#chunkmeta.chunk_id =:= Chunkid
					])).

get_all_chunkid() ->
    do(qlc:q([X#chunkmeta.chunk_id || X <- mnesia:table(chunkmeta)				     
					 ])).


insert_info(Ip, Host, Freespace, Totalspace) ->
    Row = #hostinfo{ip=Ip,
		    host=Host,
		    free_space=Freespace,
		    total_space=Totalspace
		   },
    do_trans(Row).
    
remove_info(Ip) ->    
    Oid = {hostinfo, Ip},
    F = fun() ->
		mnesia:delete(Oid)
	end,
    mnesia:transaction(F).

modify_info(Ip, Host, Freespace, Totalspace) ->
    [{hostinfo,Ip, Host, Freespace, Totalspace}] = get_info_by_ip(Ip),
    Row = #hostinfo{ip=Ip,
		    host=Host,
		    free_space=Freespace,
		    total_space=Totalspace
		   }, 
    do_trans(Row).
        


modify_host(Ip, NewHost) ->
    [{hostinfo,Ip, _, Freespace, Totalspace}] = get_info_by_ip(Ip),
    Row = #hostinfo{ip=Ip,
		    host=NewHost,
		    free_space=Freespace,
		    total_space=Totalspace
		   }, 
    do_trans(Row).

modify_freespace(Ip, NewFreespace) ->
    [{hostinfo,Ip, Host, _, Totalspace}] = get_info_by_ip(Ip),
    Row = #hostinfo{ip=Ip,
		    host=Host,
		    free_space=NewFreespace,
		    total_space=Totalspace
		   }, 
    do_trans(Row).

modify_totalspace(Ip, NewTotalspace) ->
    [{hostinfo,Ip, Host, Freespace, _}] = get_info_by_ip(Ip),
    Row = #hostinfo{ip=Ip,
		    host=Host,
		    free_space=Freespace,
		    total_space=NewTotalspace
		   }, 
    do_trans(Row).

    
get_info_by_ip(Ip) -> 
    do(qlc:q([X || X <- mnesia:table(hostinfo), 
		   X#hostinfo.ip =:= Ip
		      ])).
