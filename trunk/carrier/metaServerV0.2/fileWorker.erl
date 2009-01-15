%% Author: zyb@fit
%% Created: 2009-1-5
%% Description: TODO: Add description to fileMan
-module(fileWorker).

%%
%% Include files
%%
-include("metaformat.hrl").
-include("../include/egfs.hrl").
-import(util,[for/3,idToAtom/2]).

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
                select_attributes_from_filemeta/1,
                select_filesize_from_filemeta/1,
                select_chunklist_from_filemeta/1]).


%%
%% Exported Functions
%%
-export([startReadProcess/1,startWriteProcess/1,dieP/0,writeProcess/1,readProcess/1]).

%%
%% API Functions
%%

startReadProcess(FileID)->
    spawn(node(),fileWorker,readProcess,[FileID]).

startWriteProcess(FileID)->
    spawn(node(),fileWorker,writeProcess,[FileID]).
        
%%
%% Local Functions
%%

dieP()->
    receive
        _ ->
            1000 div 0
    end.

writeProcessTest(FileID,A,Bool)->
    
    process_flag(trap_exit, Bool),
    link(A),
    
    receive        
        {From,{allocate,_ClientID}}->    % if append,  we shall return that very last chunk.
            [Size] = select_filesize_from_filemeta(FileID),
            Rem = Size rem ?CHUNKSIZE,
 			case Rem of
                0 ->
                    io:format("new chunk allocated!~n"),
%%                     allocate_new;
                    {_, HighTime, LowTime}=now(),
					ChunkID = <<HighTime:32, LowTime:32>>,    
    				Res = select_all_from_Table(hostinfo),  % [{}{}{}{}{}]    
    				SizeOfRes = length(Res),
                    if
                        (SizeOfRes =< 0) ->
                            From!{error, "no data server active"};
                        true ->
                            random:seed(),
    						Select = random:uniform(SizeOfRes),
    						HostRecord = lists:nth(Select,Res),
							SelectedHost = HostRecord#hostinfo.procname,
                            From!{ok, ChunkID, [SelectedHost]}
                    end;
                    
                _ ->  % append
                    [ChunkList] = select_chunklist_from_filemeta(FileID),
                    LastItem = lists:last(ChunkList),                    
                    io:format("last chunk allocated!~n"),
                    
                    From!{ok,LastItem},
                    writeProcess(FileID)
            end;
        {From,{register_chunk,_ClientID, ChunkUsedSize, _NodeList}}->
            %%TODO: check ets table.
            WriteAtom = idToAtom(FileID,w),
            
            case ets:info(?FILE_WRITE_SHADOW_TABLE) of
                undefined ->
                    {error,"file does not exist"};
                [_]->
                    [{WriteAtom,FileMetaS}] = ets:lookup(?FILE_WRITE_SHADOW_TABLE,WriteAtom), % one process, one ets key,named filemeta
                    
                    FileSize = FileMetaS#filemeta.filesize + ChunkUsedSize,
                    NewMeta = FileMetaS#filemeta_s{filesize = FileSize},
                    ets:insert(WriteAtom,NewMeta),
                    From!{ok,[]},
                    writeProcess(FileID)
            end;
        {From,{close,_ClientID}}->            
            From!{ok, []}                      
    end,
    wait(writeP).

wait(Prog) ->
    receive
	Any ->
	    io:format("Process ~p received ~p~n",[Prog, Any]),
	    wait(Prog)
    end.
    


readProcess(FileID)->
    receive
        {From,{get_chunk,ChunkIdx}}->
            case select_all_from_filemeta(FileID) of
                []->
                    From!{error,"file does not exist"};
                [#filemeta{chunklist = ChunkList}]->
                    if
                        (length(ChunkList) =< ChunkIdx) ->
                            From!{error, "chunkindex is larger than chunklist size"};
                        true ->
                            ChunkID = lists:nth(ChunkIdx+1, ChunkList),
                            case select_nodeip_from_chunkmapping(ChunkID) of
                                [] -> From!{error, "chunk does not exist"};
                                [ChunkLocations] ->
                                    From!{ok, ChunkID, ChunkLocations}
                            end
                    end
            end,
            readProcess(FileID)
	end.

writeProcess(FileID)->
    receive        
        {From,{allocate,_ClientID}}->    % if append,  we shall return that very last chunk.
            [Size] = select_filesize_from_filemeta(FileID),
            Rem = Size rem ?CHUNKSIZE,
 			case Rem of
                0 ->
                    io:format("new chunk allocated!~n"),
%%                     allocate_new;
                    {_, HighTime, LowTime}=now(),
					ChunkID = <<HighTime:32, LowTime:32>>,    
    				Res = select_all_from_Table(hostinfo),  % [{}{}{}{}{}]    
    				SizeOfRes = length(Res),
                    if
                        (SizeOfRes =< 0) ->
                            From!{error, "no data server active"};
                        true ->
                            random:seed(),
    						Select = random:uniform(SizeOfRes),
    						HostRecord = lists:nth(Select,Res),
							SelectedHost = HostRecord#hostinfo.procname,
                            From!{ok, ChunkID, [SelectedHost]}
                    end;
                    
                _ ->  % append
                    [ChunkList] = select_chunklist_from_filemeta(FileID),
                    LastItem = lists:last(ChunkList),                    
                    io:format("last chunk allocated!~n"),
                    
                    From!{ok,LastItem},
                    writeProcess(FileID)
            end;
        {From,{register_chunk,_ClientID, ChunkUsedSize, _NodeList}}->
            %%TODO: check ets table.
            WriteAtom = idToAtom(FileID,w),
            
            case ets:info(WriteAtom) of
                undefined ->
                    {error,"file does not exist"};
                [_]->
                    [FileMetaS] = ets:lookup(WriteAtom,filemeta), % one process, one ets key,named filemeta
                    
                    FileSize = FileMetaS#filemeta.filesize + ChunkUsedSize,
                    NewMeta = FileMetaS#filemeta_s{filesize = FileSize},
                    ets:insert(WriteAtom,NewMeta),
                    From!{ok,[]},
                    writeProcess(FileID)
            end;
        {From,{close,_ClientID}}->            
            From!{ok, []}                      
    end.







%% write step 4: close file
do_close_o(FileID, _ClientID)->
    % delete client from filesession table  
	case select_all_from_filemeta_s(FileID) of
        [{filemeta_s, FileID, FileName, FileSize, ChunkList, CreateT, ModifyT, ACL}]->
            write_to_db({filemeta, FileID, FileName, FileSize, ChunkList, CreateT, ModifyT, ACL}),
            delete_from_db({filemeta_s, FileID}),
            {ok, []};
        _ -> 
            {ok, []}
    end.


