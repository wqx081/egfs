-module(metaserver).
-compile(export_all).
-include("metaformat.hrl").
-include("../include/egfs.hrl").
-import(lists, [reverse/1]).
-import(util,[for/3,idToAtom/2]).

-import(fileMan,[readProcess/1,writeProcess/1]).
-import(metaDB,[select_fileid_from_filemeta/1, 
                select_fileid_from_filemeta_s/1, 
                add_filemeta_s_item/2, 
                add_filemeta_item/2,
                select_all_from_Table/1, 
                select_all_from_filemeta_s/1,
                select_nodeip_from_chunkmapping/1,
                select_all_from_filemeta/1,
                select_from_hostinfo/1,
                select_chunkid_from_orphanchunk/1,
                write_to_db/1,delete_from_db/1,
                detach_from_chunk_mapping/1,
                do_register_dataserver/2,
                do_delete_filemeta/1,
                do_delete_orphanchunk_byhost/1,
                select_attributes_from_filemeta/1]).

%% lock version..
%% support write = append 
%% 
%% r----  	nofile ,error
%% 			else
%% 				nrp	,ok, create and return pid 
%%	 			readp exist, ok , return pid
%% 			// dont care writep
%% 
%% w----	nofile, create FileID, create writep
%% 			else,
%% 				writep,reject
%% 				no writep , create , write only append,
%% 			//dont care about readp
 


do_open(Filename,Modes,_ClientID)->
    %find that man first.
    case Modes of
        r-> do_read_open(Filename, _ClientID);
        w-> do_write_open(Filename, _ClientID);
        _->{error,"unkown open mode"}
    end.


do_read_open(Filename, _ClientID)->
    % mock return
    % get fileid from filemetaTable
    
    case select_fileid_from_filemeta(Filename) of
        [] -> {error, "filename does not exist"};
        % get fileid sucessfull	
        [FileID] ->
            ReadAtom = idToAtom(FileID,r),
            case whereis(ReadAtom) of
                undefined ->		% no readp , create one to read.
                    register(ReadAtom,
                             spawn(fileMan,readProcess,[FileID])
                            ),
                    
                    {ok, FileID};                
                [Pid]->			% pid exist, return it?
                    {ok, FileID}  
            end
            
    end.

do_read_open_N(Filename,_ClientID)->
    case select_fileid_from_filemeta(Filename) of
        [] -> {error, "filename does not exist"};
        % get fileid sucessfull	
        [FileID] ->
            ReadAtom = idToAtom(FileID,r),
            case whereis(ReadAtom) of
                undefined ->		% no readp , create one to read.
                    register(ReadAtom,spawn(fileMan,readProcess,[FileID])),                    
                    whereis(ReadAtom);                
                [Pid]->			% pid exist, return it?
                    Pid  
            end
    end.

do_write_open_N(Filename, _ClientID)->
    case select_fileid_from_filemeta(Filename) of
		% create file id 
        [] ->		%create new file
            {_, HighTime, LowTime}=now(),
					FileID = <<HighTime:32, LowTime:32>>,
            
            case select_all_from_filemeta(FileID) of
                []->	% ok. we got FileID now, create file record, and writeP to write.
                    WriteAtom = idToAtom(FileID,w),
            		ets:new(WriteAtom,[]),
            		
                    Row = #filemeta{fileid=FileID, filename=Filename, filesize=0, chunklist=[], 
                         createT=term_to_binary(erlang:localtime()), modifyT=term_to_binary(erlang:localtime()),acl="acl"},
                                       
                    ets:insert(WriteAtom,Row),                    
                    
                    io:format("File ~p created.~n",Filename),
                    
                    register(WriteAtom,spawn(fileMan,writeProcess,[FileID])),
                    whereis(WriteAtom);  
                [FileMetaS]->
                    {error,"fileid already exist , new fileid generator needed"}                    
            end;    
        [FileID] ->	 
            WriteAtom = idToAtom(FileID,w),
            case whereis(WriteAtom) of
                undefined ->		% no WriteAtom , create one to write.
                    register(WriteAtom,spawn(fileMan,writeProcess,[FileID])),
                    whereis(WriteAtom);                
                [Pid]->			% pid exist, write deny
                    {error,"some other client writing."}  
            end			
    end.

    
do_write_open(Filename, _ClientID)->
    % mock return    
    %TODO: Table: filesession {fileid, client} ,mode =w
	% get fileid from filemetaTable
    case select_fileid_from_filemeta(Filename) of
		% create file id 
        [] ->		%create new file
            {_, HighTime, LowTime}=now(),
					FileID = <<HighTime:32, LowTime:32>>,
            
            case select_all_from_filemeta(FileID) of
                []->	% ok. we got FileID now, create file record, and writeP to write.
                    WriteAtom = idToAtom(FileID,w),
            		ets:new(WriteAtom,[set,public,{keypos,1},named_table]), %table name = return value = WriteAtom
            		
                    Row = #filemeta{fileid=FileID, filename=Filename, filesize=0, chunklist=[], 
                         createT=term_to_binary(erlang:localtime()), modifyT=term_to_binary(erlang:localtime()),acl="acl"},
                    
                    ets:insert(WriteAtom,Row),
                    io:format("File ~p created.~n",Filename),                   
                    register(WriteAtom,spawn(fileMan,writeProcess,[FileID])),                    
                    {ok, FileID};  
                [FileMetaS]->
                    {error,"fileid already exist , new fileid generator needed"}                    
            end;    
        [FileID] ->	 %file exist
            WriteAtom = idToAtom(FileID,w),
            case whereis(WriteAtom) of
                undefined ->		% no WriteAtom , create one to write.
                    register(WriteAtom,spawn(fileMan,writeProcess,[FileID])),
                    {ok, FileID};                
                [_Pid]->			% pid exist, write deny
                    {error,"some other client writing."}  
            end
    end.

do_allocate_chunk(FileID,_ClientID)->
%%  meta server would not handle this request in later version.
%%  client shall find that man to do this job
    WriteAtom = idToAtom(FileID,w),
    WriteManPid = whereis(WriteAtom),
	%%	return value.     
    rpc(WriteManPid,{allocate,_ClientID}).
%% 	WriteManPid ! {allocate,_ClientID}.
    
do_register_chunk(FileID, _ChunkID, ChunkUsedSize, _NodeList)->
%%  meta server would not handle this request in later version.
%%  client shall find that man to do this job
    WriteAtom = idToAtom(FileID,w),
    WriteManPid = whereis(WriteAtom),
    %%	return value.  
    rpc(WriteManPid,{register_chunk,_ChunkID, ChunkUsedSize, _NodeList}).
%%     WriteManPid ! {register_chunk,_ClientID, ChunkUsedSize, _NodeList}.
    

do_close(FileID, _ClientID)->
%%  meta server would not handle this request in later version.
%%  client shall find that man to do this job
    WriteAtom = idToAtom(FileID,w),
    WriteManPid = whereis(WriteAtom),
    %%	return value.     
    rpc(WriteManPid,{close,_ClientID}).
%%     WriteManPid ! {close,_ClientID}.

do_get_chunk(FileID, ChunkIdx)->
%%  meta server would not handle this request in later version.
%%  client shall find that man to do this job
    ReadAtom = idToAtom(FileID,r),
    ReadManPid = whereis(ReadAtom),
    %%	return value.     
    rpc(ReadManPid,{get_chunk,ChunkIdx}).
%%     ReadManPid ! {get_chunk,ChunkIdx}.


%% read file attribute step 1: open file
%% read file attribute step 2: get chunk for 
do_get_fileattr(FileName)->
    [AttributeList] =select_attributes_from_filemeta(FileName),
    {ok, AttributeList}.

%%
%% 
%% host drop.
%% update chunkmapping and delete chunks
%% arg -  that host
%% return ---
do_detach_one_host(HostName)->
    detach_from_chunk_mapping(HostName). %metaDB fun


do_dataserver_bootreport(HostRecord, ChunkList)->
     do_register_dataserver(HostRecord, ChunkList). %metaDB fun

%% 
%%delete 
do_delete(FileName, _From)->
    case select_fileid_from_filemeta(FileName) of
        [] -> {error, "filename does not exist"};
        % get fileid sucessfull	
        [FileID] ->
            do_delete_filemeta(FileID)
    end.

%%
%% 
%%
do_collect_orphanchunk(HostProcName)->
    % get orphanchunk from orphanchunk table
    OrphanChunkList = select_chunkid_from_orphanchunk(HostProcName),
    % delete notified orphanchunk from orphanchunk table
    do_delete_orphanchunk_byhost(HostProcName),
    OrphanChunkList.

do_register_heartbeat(HostInfoRec)->
    Res = select_from_hostinfo(HostInfoRec#hostinfo.procname),
    Host = HostInfoRec#hostinfo.host,
    case Res of
        [{hostinfo,_Proc,Host,_TotalSpace,_FreeSpace,_Health}]->
            {_,TimeTick,_} = now(),
            NewInfo = HostInfoRec#hostinfo{health = {TimeTick,?INIT_NODE_HEALTH}},
            write_to_db(NewInfo),
            {ok,"heartbeat ok"};                
        _-> {error,"chunk does not exist"}
    end.

rpc(Pid, Request) ->
    Pid ! {self(), Request},
    receive
	Response ->
	    Response
    end.


%% 
%% debug 
%% 
do_debug(Arg) ->
    io:format("in func do_debug~n"),
    case Arg of
        wait ->
            io:format("111 ,~n"),
            timer:sleep(2000),
			io:format("222 ,~n");
        clearShadow ->
            mnesia:clear_table(filemeta_s);
        show ->
            io:format("u wana show, i give u show ,~n");
        die ->
            io:format("u wna die ,i let u die.~n"),
            1000 div 0;
        _ ->
            io:format("ohter~n")
    end.
