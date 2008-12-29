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

-define(DBNAME,'pp@pp').
%% API Functions


do_this_once() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(chunkmeta, [{attributes, record_info(fields, chunkmeta)}, {disc_copies,[node()]}]),
    mnesia:create_table(hostinfo, [{attributes, record_info(fields, hostinfo)}, {disc_copies,[node()]}]),
    mnesia:stop().

start()->
    case mnesia:create_schema([node()]) of
	ok ->
	    mnesia:start(),
	    mnesia:create_table(chunkmeta, [{attributes, record_info(fields, chunkmeta)}, {disc_copies,[node()]}]),
	    mnesia:create_table(hostinfo, [{attributes, record_info(fields, hostinfo)}, {disc_copies,[node()]}]),
	    mnesia:wait_for_tables([chunkmeta,hostinfo], 5000);
	_ -> 
	    mnesia:start(),
	    mnesia:wait_for_tables([chunkmeta,hostinfo], 5000)
    end.

    

stop()->
    mnesia:stop().

clear_tables()->
    mnesia:clear_table(chunkmeta),
    mnesia:clear_table(hostinfo).


%% construct_test_tables() ->
%%     F = fun() ->
%% 		foreach(fun mnsia:write/1, Row)
%% 	end,
%%     mnesia:transaction(F).

		


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
insert_chunk_info(Chunkid, Fileid, Path, Size) ->
    Row = #chunkmeta{file_id=Fileid,
		     chunk_id=Chunkid, 
		     path=Path,
		     length=Size,
		     create_time=erlang:localtime(), 
		     modify_time=(erlang:localtime())
		    },
    do_trans(Row).




%% remove one metadata from database where chunk_id equals Chunkid
remove_chunk_info(Chunkid)->
    Oid = {chunkmeta, Chunkid},
    F = fun() ->
		mnesia:delete(Oid)
	end,
    mnesia:transaction(F).

    
%% modify the path,size,create_time and modifytime of one metadata 
%% where chunk_id equals Chunkid
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

%% select function

%% select * from T
get_all_from_table(T)->
    do(qlc:q([X || X <- mnesia:table(T)])).

%% select * from  'chunkmeta' where 'chunk_id' = Chunkid
get_chunk_info_by_id(Chunkid) ->  
    do(qlc:q([X || X <- mnesia:table(chunkmeta), 
		   X#chunkmeta.chunk_id =:= Chunkid
		      ])).


%% select 'chunk_id' from 'chunkmeta' where 'path' = Path
get_chunk_id_by_path(Path) ->
    do(qlc:q([X#chunkmeta.chunk_id || X <- mnesia:table(chunkmeta),
                                   X#chunkmeta.path =:= Path
					 ])).   
get_file_id_by_chunk_id(Chunkid) ->
    do(qlc:q([X#chunkmeta.file_id || X <- mnesia:table(chunkmeta), 
				     X#chunkmeta.chunk_id =:= Chunkid
					])).

get_all_chunkid() ->
    do(qlc:q([X#chunkmeta.chunk_id || X <- mnesia:table(chunkmeta)				     
					 ])).


insert_host_info(Ip, Hostname, Freespace, Totalspace) ->
    Row = #hostinfo{ip=Ip,
		    host_name=Hostname,
		    free_space=Freespace,
		    total_space=Totalspace
		   },
    do_trans(Row).
    
remove_host_info(Ip) ->    
    Oid = {hostinfo, Ip},
    F = fun() ->
		mnesia:delete(Oid)
	end,
    mnesia:transaction(F).

modify_host_info(Ip, Hostname, Freespace, Totalspace) ->
    [{hostinfo, Ip, _, _, _}] = get_host_info_by_ip(Ip),
    Row = #hostinfo{ip=Ip,
		    host_name=Hostname,
		    free_space=Freespace,
		    total_space=Totalspace
		   }, 
    do_trans(Row).
        


modify_host_hostname(Ip, NewHostname) ->
    [{hostinfo,Ip, _, Freespace, Totalspace}] = get_host_info_by_ip(Ip),
    Row = #hostinfo{ip=Ip,
		    host_name=NewHostname,
		    free_space=Freespace,
		    total_space=Totalspace
		   }, 
    do_trans(Row).

modify_host_freespace(Ip, NewFreespace) ->
    [{hostinfo,Ip, Hostname, _, Totalspace}] = get_host_info_by_ip(Ip),
    Row = #hostinfo{ip=Ip,
		    host_name=Hostname,
		    free_space=NewFreespace,
		    total_space=Totalspace
		   }, 
    do_trans(Row).

modify_host_totalspace(Ip, NewTotalspace) ->
    [{hostinfo,Ip, Hostname, Freespace, _}] = get_host_info_by_ip(Ip),
    Row = #hostinfo{ip=Ip,
		    host_name=Hostname,
		    free_space=Freespace,
		    total_space=NewTotalspace
		   }, 
    do_trans(Row).

    
get_host_info_by_ip(Ip) -> 
    do(qlc:q([X || X <- mnesia:table(hostinfo), 
		   X#hostinfo.ip =:= Ip
		      ])).



example_tables() ->
    [{chunkmeta, 1, 1, '/hom/pp/egfs/',1024, erlang:localtime(), erlang:localtime()},
     {hostinfo, '192.168.0.118', 'pp@pp', 1024000, 2048000}     
    ].
