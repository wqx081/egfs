%% Author: zyb
%% Created: 2008-12-22
%% Description: TODO: Add description to metaDB

-module(metaDB).
-import(lists, [foreach/2]).
-import(util,[for/3]).
%%
%% Include files
%%
-include("metaformat.hrl").
-include("../include/egfs.hrl").
-include_lib("stdlib/include/qlc.hrl").
%%
%% Exported Functions
%%
%-export([]).
-compile(export_all).

%%
%% API Functions
%%

do_this_once() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(filemeta, 	%table name 
                        [			
                         {attributes, record_info(fields, filemeta)},%table content
                         {disc_copies,[node()]}                         
                        ]
                       ),
    mnesia:create_table(filemeta_s, 	%table name 
                        [			
                         {attributes, record_info(fields, filemeta_s)},%table content
                         {disc_copies,[node()]}                         
                        ]
                       ),
    mnesia:create_table(chunkmapping, [{attributes, record_info(fields, chunkmapping)},
                                       {disc_copies,[node()]}
                                      ]),
    mnesia:create_table(hostinfo, [{attributes, record_info(fields,hostinfo)},
                                      {disc_copies,[node()]}
                                     ]),
    
    mnesia:create_table(metalog, [{attributes, record_info(fields,metalog)},
                                      {disc_copies,[node()]}
                                     ]),
    mnesia:create_table(orphanchunk, [{type,bag},{attributes, record_info(fields,orphanchunk)},
                                      {disc_copies,[node()]}
                                     ]),
	
    mnesia:stop().

start_mnesia()->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="start_mnesia",logarg=[]},
    mnesia:start(),    
    mnesia:wait_for_tables([filemeta,filemeta_s,chunkmapping,hostinfo,metalog,orphanchunk], 14000),
    logF(LOG).

%%
%% Local Functions
%%


do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Val} = mnesia:transaction(F),
    Val.

example_tables() ->
    [
     {hostinfo,{data_server, 'lt@lt'},abc,1000000,2000000}
    ].
clear_tables()->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="cleart_tables/0",logarg=[]},
    logF(LOG),

    mnesia:clear_table(filemeta),
    mnesia:clear_table(filemeta_s),
    mnesia:clear_table(hostinfo),
    mnesia:clear_table(chunkmapping).


reset_tables() ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="reset_tables/0",logarg=[]},
    logF(LOG),
    mnesia:clear_table(filemeta),
    mnesia:clear_table(chunkmapping),
    mnesia:clear_table(filemeta_s),
    mnesia:clear_table(hostinfo),

    F = fun() ->
		foreach(fun mnesia:write/1, example_tables())
		end,
    mnesia:transaction(F).

%filemeta    {fileid	client}
%add item
add_filemeta_item(Fileid, FileName) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="add_filemeta_item/2",logarg=[Fileid,FileName]},
    logF(LOG),
    Row = #filemeta{fileid=Fileid, filename=FileName, filesize=0, chunklist=[], 
                         createT=term_to_binary(erlang:localtime()), modifyT=term_to_binary(erlang:localtime()),acl="acl"},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).

%filemeta_s    {fileid	client}
%add item
add_filemeta_s_item(Fileid, FileName) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="add_filemeta_s_item/2",logarg=[Fileid,FileName]},
    logF(LOG),
    Row = #filemeta_s{fileid=Fileid, filename=FileName, filesize=0, chunklist=[], 
                         createT=term_to_binary(erlang:localtime()), modifyT=term_to_binary(erlang:localtime()),acl="acl"},
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).


%add_orphan_item
add_orphan_item(Chunkid,Chunklocation)->
    I = #orphanchunk{chunkid=Chunkid,chunklocation=Chunklocation},
    F = fun() ->
		mnesia:write(I)
	end,
 	mnesia:transaction(F).


%% write a record 
%% 
write_to_db(X)->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="write_to_db/1",logarg=[X]},
    logF(LOG),
    
    %io:format("inside write to db"),
    F = fun() ->
		mnesia:write(X)
	end,
    mnesia:transaction(F).





%% delete a record 
%%
delete_object_from_db(X)->
	LOG = #metalog{logtime = calendar:local_time(),logfunc="delete_object_from_db/1",logarg=[X]},
    logF(LOG),
    %io:format("inside delete from db"),
    F = fun() ->
                mnesia:delete_object(X)
        end,
    mnesia:transaction(F).


delete_from_db(X)->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="delete_from_db/1",logarg=[X]},
    logF(LOG),
    %io:format("inside delete from db"),
    F = fun() ->
                mnesia:delete(X)
        end,
    mnesia:transaction(F).
  
delete_from_db(listrecord,[X|T])->
    delete_from_db(X),
    delete_from_db(listrecord,T);
delete_from_db(listrecord,[])->
	done.

delete_object_from_db(listrecord,[X|T])->
    delete_object_from_db(X),
    delete_object_from_db(listrecord,T);
delete_object_from_db(listrecord,[])->
	done.

%%------------------------------------------------------------------------------------------
%% select function
%% all kinds 
%%------------------------------------------------------------------------------------------ 
select_all_from_Table(T)->    
    do(qlc:q([
              X||X<-mnesia:table(T)
              ])).  %result [L]

select_all_from_filemeta(FileID) ->    %result [L]
    do(qlc:q([
              X||X<-mnesia:table(filemeta),X#filemeta.fileid =:= FileID
              ])).

select_all_from_filemeta_s(FileID)->
    do(qlc:q([
              X||X<-mnesia:table(filemeta_s),X#filemeta_s.fileid =:= FileID
              ])).


select_from_hostinfo(ProcName)->
    do(qlc:q([
              X||X<-mnesia:table(hostinfo),X#hostinfo.procname =:= ProcName
              ])).


select_chunkid_from_orphanchunk(Host) ->
    do(qlc:q([
              X#orphanchunk.chunkid||X<-mnesia:table(orphanchunk),X#orphanchunk.chunklocation =:= Host
              ])).


select_all_from_orphanchunk(Host) ->
    do(qlc:q([
              X||X<-mnesia:table(orphanchunk),X#orphanchunk.chunklocation =:= Host
              ])).

%FileName - > fileid
% @spec select_fileid_from_filemeta(FileName) ->  fileid
% fileid -> binary().

select_fileid_from_filemeta(FileName) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="select_fileid_from_filemeta/1",logarg=[FileName]},
    logF(LOG),
    
    do(qlc:q([X#filemeta.fileid || X <- mnesia:table(filemeta),
                                   X#filemeta.filename =:= FileName                                   
                                   ])).   %result [L]

select_fileid_from_filemeta_s(FileName) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="select_fileid_from_filemeta_s/1",logarg=[FileName]},
    logF(LOG),
    do(qlc:q([X#filemeta_s.fileid || X <- mnesia:table(filemeta_s),
                                   X#filemeta_s.filename =:= FileName                                   
                                   ])).   %result [L]

select_nodeip_from_chunkmapping(ChunkID) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="select_nodeip_from_chunkmapping",logarg=[ChunkID]},
    logF(LOG),
    do(qlc:q([X#chunkmapping.chunklocations || X <- mnesia:table(chunkmapping),
                                   X#chunkmapping.chunkid =:= ChunkID
             ])).   %result [L]

%clear chunks from filemeta
reset_file_from_filemeta(Fileid) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="reset_file_from_filemeta",logarg=[Fileid]},
    logF(LOG),
    [{filemeta, FileID, FileName, _, _, TimeCreated, _, ACL }] =
    do(qlc:q([X || X <- mnesia:table(filemeta),
                                   X#filemeta.fileid =:= Fileid                                   
                                   ])),

	Row = {filemeta, FileID, FileName, 0, [], TimeCreated, term_to_binary(erlang:localtime()), ACL },
    F = fun() ->
		mnesia:write(Row)
	end,
    mnesia:transaction(F).

% detach_from_chunk_mapping
% arg, Host name
detach_from_chunk_mapping(Host) ->
    DelHost =
        fun(ChunkMapping, Acc) ->
                ChunkLoc = ChunkMapping#chunkmapping.chunklocations,
                Guard = lists:member(Host,ChunkLoc),
                if Guard =:= true ->
                       ChunkLocList = ChunkLoc -- [Host],
                       ok = mnesia:write(ChunkMapping#chunkmapping{chunklocations = ChunkLocList}),
                       if 
                           ChunkLocList =:= [] ->
                               Acc ++ [ChunkMapping#chunkmapping.chunkid];
                           true ->
                               Acc
                       end; 
                   true ->
                    	Acc   
                end
        end,
    DoDel = fun() -> mnesia:foldl(DelHost, [], chunkmapping, write) end,
    mnesia:transaction(DoDel).



%% powerfull log function
logF(X)->
    F = fun() ->
		mnesia:write(X)
	end,
mnesia:transaction(F).

%% --------------------------------------------------------------------
%% Function: 
%% Description: 
%% Returns: 
%% --------------------------------------------------------------------


%% --------------------------------------------------------------------
%% Function: reset_file_from_filemeta/1
%% Description: 
%% Argument: Fileid  @type <<binary:64>>
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------


%%		TODO:  log of all function. 

modifyHostLocation() ->
    ModifyLoc =
        fun(ChunkMapping, Acc) when is_atom(ChunkMapping#chunkmapping.chunklocations)->
                ChunkLoc = ChunkMapping#chunkmapping.chunklocations,
                mnesia:write(ChunkMapping#chunkmapping{chunklocations = [ChunkLoc]}),
                Acc;
           (_, Acc) ->
                Acc
        end,
    ModifyFun = fun() -> mnesia:foldl(ModifyLoc, [], chunkmapping, write) end,
    mnesia:transaction(ModifyFun).


%% 
do_register_dataserver(HostRecord,ChunkList)->
	X = select_from_hostinfo(HostRecord#hostinfo.procname),
    case X of
        []->
            % insert into hostinfo;
            F = fun() ->
                        mnesia:write(HostRecord)
                        end,
            mnesia:transaction(F),
        	do_register_boot_chunks(HostRecord#hostinfo.procname,ChunkList);
        
        _->	% host existed.
            {error, "host collision"}
    end.
    
%%
%%
do_register_boot_chunks(HostProcName,ChunkList)->
   AddHost =
        fun(ChunkMapping, Acc) ->
                ChunkID = ChunkMapping#chunkmapping.chunkid,                
                Guard = lists:member(ChunkID,Acc),                
                if Guard =:= true ->
%%                        Acc = ChunkList--[ChunkID],
                       ChunkLocations = 
                           lists:usort(ChunkMapping#chunkmapping.chunklocations++[HostProcName]),
                       
                       ok = mnesia:write(
                              ChunkMapping#chunkmapping{chunklocations = ChunkLocations}),
                       Acc--[ChunkID];
                   true ->
                    	Acc
                end
        end,
    DoAdd = fun() -> mnesia:foldl(AddHost, ChunkList, chunkmapping, write) end,
    case mnesia:transaction(DoAdd) of
        {atomic, UnusedChunkList} ->
            {ok, UnusedChunkList};
        {aborted, _} ->
            {error, "Mnesia Transaction Abort!"}
   end.



do_delete_filemeta(FileID)->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="do_delete_filemeta/1",logarg=[FileID]},
    logF(LOG),
    delete_from_db({filemeta,FileID}),				%return {atomic,ok} . always
    {ok,"File deleted"}.
    

% find orphanchunk every day.
do_find_orphanchunk()->
    % Get all chunks of chunkmapping table
    GetAllChunkIdList = 
        fun(ChunkMapping,Acm)->
                [ChunkMapping#chunkmapping.chunkid | Acm]
        end,
    DogetAllChunkIdList = fun() -> mnesia:foldl(GetAllChunkIdList, [], chunkmapping) end,
    {atomic, AllChunkIdList} = mnesia:transaction(DogetAllChunkIdList),
    
    % filter out used chunks according to filemeta table
	GetUsedChunkIdList =
        fun(FileMeta, Acc) ->                
                Acc--FileMeta#filemeta.chunklist                
        end,        
    DogetUsedChunkIdList = fun() -> mnesia:foldl(GetUsedChunkIdList, AllChunkIdList, filemeta) end,
    {atomic, OrphanChunk} = mnesia:transaction(DogetUsedChunkIdList),
    
    GetOrphanPair = 
        fun(X) ->
                NodeIpList = select_nodeip_from_chunkmapping(X),
                [write_to_db({orphanchunk,X,Y}) || Y<-NodeIpList],
                delete_from_db({chunkmapping,X})
        end,
    [GetOrphanPair(X)||X<-OrphanChunk].

% delete orphanchunk record in orphanchunk table by host
do_delete_orphanchunk_byhost(HostProcName)->
	X = select_all_from_orphanchunk(HostProcName),
	io:format("~p ~n", [list_to_tuple(X)]),
	delete_object_from_db(listrecord,X).

% find orphanchunk in orphanchunk table by host
do_get_orphanchunk_byhost(HostProcName) ->
    select_all_from_orphanchunk(HostProcName).

