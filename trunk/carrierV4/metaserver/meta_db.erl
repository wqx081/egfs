%% Author: zyb
%% Created: 2008-12-22
%% Description: TODO: Add description to metaDB

-module(meta_db).
-import(lists, [foreach/2]).
-import(util,[for/3]).
%%
%% Include files
%%
-include("../include/header.hrl").
-include_lib("stdlib/include/qlc.hrl").
%%
%% Exported Functions
%%
%-export([]).
-compile(export_all).

%%
%% API Functions
%%


create_a_table()->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(hostinfo, [{attributes, record_info(fields,hostinfo)},
                                      {disc_copies,[node()]}
                                     ]),
    mnesia:stop().

do_this_once() ->
    mnesia:start(),
     
    mnesia:create_table(filemeta, 	%table name 
                        [			
                         {attributes, record_info(fields, filemeta)},%table content
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

    LOG = #metalog{logtime = calendar:local_time(),logfunc="start_mnesia",logarg=[]},
    mnesia:wait_for_tables([filemeta,chunkmapping,hostinfo,metalog,orphanchunk], 14000),
    reset_tables(),
    logF(LOG).


start_mnesia()->
    
    case mnesia:create_schema([node()]) of
        ok ->
            do_this_once();
        _ ->
            LOG = #metalog{logtime = calendar:local_time(),logfunc="start_mnesia",logarg=[]},
            mnesia:start(),
            mnesia:wait_for_tables([filemeta,chunkmapping,hostinfo,metalog,orphanchunk], 14000),
            logF(LOG)
    end,
    
    start_mnesia_ok.

%% Local Functions
%%


do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Val} = mnesia:transaction(F),
    Val.

%%-record(filemeta,{id,name,chunklist,parent,size,type,access,atime,mtime,ctime,mode,links,inode,uid,gid})
example_tables() ->
    [
     %%{hostinfo,{data_server,lt@lt},{192,168,0,111},1000000,2000000,{0,100}},
     {filemeta,lib_uuid:gen(),"/",[],[],-1,directory,read,0,erlang:localtime(),erlang:localtime(),0,1,0,0,0}
    ].

clear_tables()->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="cleart_tables/0",logarg=[]},
    logF(LOG),

    mnesia:clear_table(filemeta),    
    mnesia:clear_table(hostinfo),
    mnesia:clear_table(chunkmapping).


reset_tables() ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="reset_tables/0",logarg=[]},
    logF(LOG),    
    mnesia:clear_table(filemeta),
    mnesia:clear_table(chunkmapping),    
    mnesia:clear_table(hostinfo),
    F = fun() ->
		foreach(fun mnesia:write/1, example_tables())
		end,
    mnesia:transaction(F).

%add_orphan_item
%% add_orphan_item(Chunkid,Chunklocation)->
%%     I = #orphanchunk{chunkid=Chunkid,chunklocation=Chunklocation},
%%     F = fun() ->
%% 		mnesia:write(I)
%% 	end,
%%  	mnesia:transaction(F).

%% write a record 
%% 
write_to_db(X)->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="write_to_db/1",logarg=[X]},
    logF(LOG),
    
    %io:format("inside write to db"),
    F = fun() ->
		mnesia:write(X)
	end,
    {atomic,Val}=mnesia:transaction(F),
    Val.

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

  
%% delete_from_db(listrecord,[X|T])->
%%     delete_from_db(X),
%%     delete_from_db(listrecord,T);
%% delete_from_db(listrecord,[])->
%% 	done.
%% 
%% delete_object_from_db(listrecord,[X|T])->
%%     delete_object_from_db(X),
%%     delete_object_from_db(listrecord,T);
%% delete_object_from_db(listrecord,[])->
%% 	done.


delete_hostinfo_item(HostName) ->
	Oid = {hostinfo, HostName},
	F = fun() ->
		mnesia:delete(Oid)
	end,
	{atomic, Val} = mnesia:transaction(F),
	Val.

%%------------------------------------------------------------------------------------------
%% select function
%% all kinds 
%%------------------------------------------------------------------------------------------ 
select_all_from_Table(T)->    
    do(qlc:q([
              X||X<-mnesia:table(T)
              ])).  %result [L]
%%[ {},{},{},{}  ]

select_all_from_filemeta_byID(FileID) ->    %result [L]
    do(qlc:q([
              X||X<-mnesia:table(filemeta),X#filemeta.id =:= FileID
              ])).

select_all_from_filemeta_byName(FileName) ->
  do(qlc:q([
              X||X<-mnesia:table(filemeta),X#filemeta.name =:= FileName
              ])).

select_all_from_hostinfo_byHostname(Hostname)->
    do(qlc:q([
              X||X<-mnesia:table(hostinfo),X#hostinfo.hostname =:= Hostname
              ])).
%% 
select_hostname_from_hostinfo()->
    do(qlc:q([
              X#hostinfo.hostname||X<-mnesia:table(hostinfo)
              ])).

select_nodename_from_hostinfo(Hostname)->
    do(qlc:q([
              X#hostinfo.nodename||X<-mnesia:table(hostinfo),X#hostinfo.hostname =:= Hostname
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

select_hosts_from_chunkmapping_id(ChunkID) ->    
    do(qlc:q([X#chunkmapping.chunklocations||X<-mnesia:table(chunkmapping),X#chunkmapping.chunkid =:= ChunkID])).

select_filesize_from_filemeta(FileId) ->
	LOG = #metalog{logtime = calendar:local_time(),logfunc="select_filesize_from_filemeta/1",logarg=[FileId]},
    logF(LOG),    
    do(qlc:q([X#filemeta.size || X <- mnesia:table(filemeta),
                                   X#filemeta.id =:= FileId                                   
                                   ])).   %result [L]

select_chunklist_from_filemeta(FileId) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="select_chunklist_from_filemeta/1",logarg=[FileId]},
    logF(LOG),    
    do(qlc:q([X#filemeta.chunklist || X <- mnesia:table(filemeta),
                                   X#filemeta.id =:= FileId                                   
                                   ])).   %result [L]

select_fileid_from_filemeta(FileName) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="select_fileid_from_filemeta/1",logarg=[FileName]},
    logF(LOG),
    
    do(qlc:q([X#filemeta.id || X <- mnesia:table(filemeta),
                                   X#filemeta.name =:= FileName                                   
                                   ])).   %result [L]

%% select_fileid_from_filemeta_s(FileName) ->
%%     LOG = #metalog{logtime = calendar:local_time(),logfunc="select_fileid_from_filemeta_s/1",logarg=[FileName]},
%%     logF(LOG),
%%     do(qlc:q([X#filemeta_s.fileid || X <- mnesia:table(filemeta_s),
%%                                    X#filemeta_s.filename =:= FileName                                   
%%                                    ])).   %result [L]

select_nodeip_from_chunkmapping(ChunkID) ->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="select_nodeip_from_chunkmapping",logarg=[ChunkID]},
    logF(LOG),
    do(qlc:q([X#chunkmapping.chunklocations || X <- mnesia:table(chunkmapping),
                                   X#chunkmapping.chunkid =:= ChunkID
             ])).   %result [L]


select_item_from_chunkmapping_id(ChunkID) ->    
    do(qlc:q([X||X<-mnesia:table(chunkmapping),X#chunkmapping.chunkid =:= ChunkID])).

select_chunkid_from_chunkmapping()->
    do(
      qlc:q([X#chunkmapping.chunkid||X<-mnesia:table(chunkmapping)])
      ).



select_nodename_from_hostinfo()->
    do(
      qlc:q([X#hostinfo.nodename||X<-mnesia:table(hostinfo)])
      ).
    


%% %clear chunks from filemeta
%% reset_file_from_filemeta(Fileid) ->
%%     LOG = #metalog{logtime = calendar:local_time(),logfunc="reset_file_from_filemeta",logarg=[Fileid]},
%%     logF(LOG),
%%     [{filemeta, FileID, FileName, _, _, TimeCreated, _, ACL }] =
%%     do(qlc:q([X || X <- mnesia:table(filemeta),
%%                                    X#filemeta.id =:= Fileid                                   
%%                                    ])),
%% 
%% 	Row = {filemeta, FileID, FileName, 0, [], TimeCreated, term_to_binary(erlang:localtime()), ACL },
%%     F = fun() ->
%% 		mnesia:write(Row)
%% 	end,
%%     mnesia:transaction(F).

% detach_from_chunk_mapping
% arg, Host name
%%TODO, 
detach_from_chunk_mapping(Host) ->
    DelHost =
        fun(ChunkMapping, Acc) ->
                ChunkLoc = ChunkMapping#chunkmapping.chunklocations,
                %%TODO, rewrite these,
                Guard = lists:member(Host,ChunkLoc),
                if Guard =:= true ->
                       ChunkLocList = ChunkLoc -- [Host],
                       case ChunkLocList of
                           []->
                               %delete chunkid record
                               ok = mnesia:delete(ChunkMapping);
                           _Any->
                               ok = mnesia:write(ChunkMapping#chunkmapping{chunklocations = ChunkLocList})
                       end
                end,
                Acc 
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


%% add this node'chunklocation to chunk mapping table. 
%% if chunkID in this node don't exist in table chunk mapping,  this chunkid is not added. (why?) 
%% 
%% called when, bootreport 
do_register_dataserver(HostName,ChunkList)->
   AddHost =
        fun(ChunkMapping, Acc) ->
                ChunkID = ChunkMapping#chunkmapping.chunkid,
                Guard = lists:member(ChunkID,Acc),                
                if Guard =:= true ->
%%                        Acc = ChunkList--[ChunkID],
                       error_logger:info_msg("Old :~p~n",[ChunkMapping#chunkmapping.chunklocations]),
                       ChunkLocations =lists:delete(HostName,ChunkMapping#chunkmapping.chunklocations)++[HostName], %%better solution
                       error_logger:info_msg("chunklocations: ~p~n,",[ChunkLocations]),
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



do_delete_filemeta_byID(FileID)->
    LOG = #metalog{logtime = calendar:local_time(),logfunc="do_delete_filemeta/1",logarg=[FileID]},
    logF(LOG),
    delete_from_db({filemeta,FileID}).				%return {atomic,ok|Val.} .  | {aborted, Reason}
    
    

% delete orphanchunk record in orphanchunk table by host
%% do_delete_orphanchunk_byhost(HostProcName)->
%% 	X = select_all_from_orphanchunk(HostProcName),
%% 	io:format("~p ~n", [list_to_tuple(X)]),
%% 	delete_object_from_db(listrecord,X).

% find orphanchunk in orphanchunk table by host
%% do_get_orphanchunk_byhost(HostProcName) ->
%%     select_all_from_orphanchunk(HostProcName).


%%====================================================================
%% add item into table

%% add file info to table: filemeta & chunkmapping
%%====================================================================
add_a_file_record(FileRecord, ChunkMappingRecords) ->
%%     error_logger:info_msg("~~~~ in add_a_file_record~~~~n"),
    CurrentT = erlang:localtime(),
    %%TODO: create time & modify time . . mode append,. 
    Row = FileRecord#filemeta{	ctime=CurrentT, 
			                    mtime=CurrentT,
                                type = regular,
                                parent = get_id(filename:dirname(FileRecord#filemeta.name))
                             },
    %%uuid never repeat    
	F = fun() ->
		mnesia:write(Row),
		lists:foreach(fun mnesia:write/1, ChunkMappingRecords)   
	end,
    {atomic, Val} = mnesia:transaction(F),
%%     error_logger:info_msg("write into db success"),
	Val.


append_a_file_record(FileRecord,ChunkMappingRecords) ->
    CurrentT = erlang:localtime(),
    [Old] = meta_db:select_all_from_filemeta_byID(FileRecord#filemeta.id),
    New = Old#filemeta{
                       mtime=CurrentT,
                       size = FileRecord#filemeta.size,
                       chunklist = Old#filemeta.chunklist++FileRecord#filemeta.chunklist
                      },
    F = fun() ->
		mnesia:write(New),
		lists:foreach(fun mnesia:write/1, ChunkMappingRecords)   
	end,
    {atomic, Val} = mnesia:transaction(F),
	Val.
    

%% add record hostinfo to table hostinfo, no chunkmapping table change .
%% when meta_host , gen_server_call, register_dataserver
add_hostinfo_item(HostName,NodeName, FreeSpace, TotalSpace, Status,From) ->
%%     io:format("in side add_hostiofo_item.~n"),
	Row = #hostinfo{hostname=HostName, nodename = NodeName ,freespace=FreeSpace, totalspace=TotalSpace, status=Status,life=?HOST_INIT_LIFE},
%% 	io:format("From : ~p~n",[From]),
    
	case select_all_from_hostinfo_byHostname(HostName) of
		[] -> 
%%             io:format("first time register of this host: ~p~n",[HostName]),    
			write_to_db(Row);
		[_Any] ->
%%             io:format("host with same name was deleted first,~p~n",[HostName]),
            delete_from_db({hostinfo,HostName}),            
%%             io:format("delete ok , begin to write,~n"),
            write_to_db(Row)
	end.



%%-record(filemeta,{id,name,chunklist,parent,size,type,access,atime,mtime,ctime,mode,links,inode,uid,gid})
%% there's no "/" after DirName, 
add_new_dir(ID,DirName,ParentID) ->
    Row = #filemeta{id=ID,name=DirName,chunklist=[],parent=ParentID,size = -1 ,type=directory,mtime = calendar:local_time(),ctime = calendar:local_time()},
    F = fun() ->
                mnesia:write(Row)
        end,
    mnesia:transaction(F),
    ok.



select_random_one_from_hostinfo()->
    Hosts = do(qlc:q([X#hostinfo.hostname||X<-mnesia:table(hostinfo)])),
	case length(Hosts) of
	    0 ->
			[];
		Number ->
			{A1,A2,A3}=now(),
			random:seed(A1,A2,A3), 
    		Position = random:uniform(Number),	
			[lists:nth(Position,Hosts)]
	end.


%% with end '/' slash version. 
%% get_tag(FileName) ->    
%%     L = length(FileName),    
%% %%     io:format("~p~n",[L]),
%%     case (string:equal(string:right(FileName,1),"/"))andalso L>1 of
%%        true->           
%% %%            io:format("true,~p~n",[FileName]),
%%            get_tag(string:substr(FileName,1,L-1));
%%        false->
%% %%            io:format(",~p~n",[FileName]),
%%            Result = do(qlc:q([X#filemeta.type||X<-mnesia:table(filemeta), X#filemeta.name=:=FileName])),
%%            case Result of
%%                [] ->
%%                    null;
%%                [Tag] ->
%%                    Tag
%%            end
%%     end.


get_tag(FileName) ->
    Result = do(qlc:q([X#filemeta.type||X<-mnesia:table(filemeta), X#filemeta.name=:=FileName])),
     case Result of
        [] ->
            null;
        [Tag] ->
            Tag
    end
.   


get_tag_by_id(ID) ->
    Result = do(qlc:q([X#filemeta.type||X<-mnesia:table(filemeta), X#filemeta.id=:=ID])),
    case Result of
        [] ->
            null;
        [Tag] ->
            Tag
    end
.
get_name(ID) ->
    Result = do(qlc:q([X#filemeta.name||X<-mnesia:table(filemeta), X#filemeta.id=:=ID])),
    case Result of
        [] ->
            null;
        [Name|_T] ->
            Name
    end
.

get_id(FileName) ->
    Result = do(qlc:q([X#filemeta.id||X<-mnesia:table(filemeta), X#filemeta.name=:=FileName])),
    case Result of
        [] ->
            null;
        [ID|_T] ->
            ID
    end
.
get_time(ID) ->
    Result = do(qlc:q([{X#filemeta.ctime,X#filemeta.mtime}||X<-mnesia:table(filemeta), X#filemeta.id=:=ID])),
    case Result of
        [] ->
            null;
        [{CT,MT}|_T] ->
            {CT,MT}
    end
.



%% easier one , useing powerful qlc.
get_all_sub_files_byID(FileID) ->
    case get_tag_by_id(FileID) of
        file ->
            [{file,FileID,get_name(FileID)}];		%% [{tag,id,name}]
        dir ->
            FileName = get_name(FileID),
            L = length(FileName),
            Result = do(qlc:q([
                               {X#filemeta.type,X#filemeta.id,X#filemeta.name}
                      		||X<-mnesia:table(filemeta), 
                              string:equal(string:left(X#filemeta.name,L+1),FileName++"/"),
                              X#filemeta.name =/= FileName
                      ])),
            Result;				%%[{}{}{}{}{}{}{}{}{}{}]
        null->
            [{wtf,wtf,wtf}]
    end.

get_all_sub_files_byName(FileName) ->
    case get_tag(FileName) of
        regular ->
            [{regular,get_id(FileName),FileName}];		%% [{tag,id,name}]
        directory ->            
            L = length(FileName),
            Result = do(qlc:q([
                               {X#filemeta.type,X#filemeta.id,X#filemeta.name}
                      		||X<-mnesia:table(filemeta), 
                              string:equal(string:left(X#filemeta.name,L+1),FileName++"/"),
                              X#filemeta.name =/= FileName
                      ])),
            Result;				%%[{}{}{}{}{}{}{}{}{}{}]
        null->
            [{wtf,wtf,wtf}]
    end.

%% hiatus version. 
%% get_all_sub_files(FileID) ->
%%     case meta_db:get_tag_by_id(FileID) of
%%         file ->
%%             [[FileID],[]];
%%         dir ->
%%             [DirectSubFile,DirectSubDir] = get_direct_sub_files(FileID),
%%             [DirectSubDir_File, DirectSubDir_Dir] = get_all_sub_files(list, DirectSubDir),
%%             [lists:append(DirectSubFile, DirectSubDir_File),lists:append(DirectSubDir, DirectSubDir_Dir)]
%%     end
%% .
%% 
%% get_all_sub_files(list, []) ->
%%     [[],[]];
%% get_all_sub_files(list, FileIDList) ->
%%     [Head|Left] = FileIDList,
%%     [HeadSubFile,HeadSubDir] = get_all_sub_files(Head),
%%     [LeftSubFile,LeftSubDir] = get_all_sub_files(list, Left),
%%     [lists:append(HeadSubFile, LeftSubFile),lists:append(HeadSubDir, LeftSubDir)]
%% .
%% seperate_file_dir([]) ->
%%     [[],[],[],[]];
%% seperate_file_dir(FileList) ->
%%     [{Tag, ID,Name}|Left] = FileList,
%%     [LeftFiles,LeftDirs,LeftFileNames,LeftDirNames] = seperate_file_dir(Left),
%%     case Tag of
%%         file ->
%%             [lists:append([ID],LeftFiles),LeftDirs,lists:append([Name],LeftFileNames),LeftDirNames];
%%         dir ->
%%             [LeftFiles,lists:append([ID],LeftDirs),LeftFileNames,lists:append([Name],LeftDirNames)]
%%     end
%% .
%% 


get_order_direct_sub_files(FileID) ->
    Q1 = qlc:q([
                {X#filemeta.type,X#filemeta.id,X#filemeta.name}
                      		||X<-mnesia:table(filemeta), X#filemeta.parent=:=FileID
                      ]),
    Q2 = qlc:keysort(1,Q1,{order,descending}), %% 1: filemeta.tag  descending:f>d, file(f) first,then dirs(d)    

    do(Q2).

get_direct_sub_files(FileID) ->
    Result = do(
               qlc:q([{X#filemeta.type,X#filemeta.id,X#filemeta.name}
                      		||X<-mnesia:table(filemeta), X#filemeta.parent=:=FileID
                      ])
               ),
	Result.

%% checked it's a dir before function called. 
%% get_all_dir_sub_files(FileName)->
%%     L = length(FileName),
%%     Result = do(qlc:q([
%%                                {X#filemeta.type,X#filemeta.id,X#filemeta.name}
%%                       		||X<-mnesia:table(filemeta), 
%%                               string:equal(string:left(X#filemeta.name,L+1),FileName++"/"),  %% mabe slow
%%                               X#filemeta.name =/= FileName
%%                       ])),
%%     Result.				%%[{}{}{}{}{}{}{}{}{}{}]
%%     



%% spec add_heartbeat_info(HostName,State) -> {ok,_} |{error,_}
update_heartbeat(HostName,State) ->
    case select_all_from_hostinfo_byHostname(HostName) of
        [Host]-> %%update            
            Row = Host#hostinfo{status = State,life = ?HOST_INIT_LIFE},
    		F = fun() ->
				mnesia:write(Row)
			end,
    		{atomic,_Val}=mnesia:transaction(F),           
%%          write_to_db(Row), too many log ...
            ok;
        []->
%%             error_logger:error_msg("no info of this host, neeeedreport,"),
            needreport
    
    end.



%% 
%% do_fix()->    
%%     [R] = select_all_from_filemeta_byName("/"),    
%%     New = R#filemeta{parent = []},
%%     write_to_db(New).

%% old_dofix()->
%%     X = select_all_from_Table(filemeta),%%[{},{},{}]
%%     dofix(X).
%% dofix(X) when X =:= [] ->
%%     ture;
%% dofix(X) ->
%%     [H|T] = X,
%%     New = H#filemeta{parent = get_id(filename:dirname(H#filemeta.filename))},
%%     write_to_db(New),
%%     dofix(T).
     
    
    
            