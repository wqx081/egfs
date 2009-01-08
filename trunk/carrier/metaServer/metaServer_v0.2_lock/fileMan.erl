%% Author: zyb@fit
%% Created: 2009-1-5
%% Description: TODO: Add description to fileMan
-module(fileMan).

%%
%% Include files
%%
-include("metaformat.hrl").
-include("../../include/egfs.hrl").

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
                select_filesize_from_filemeta/1]).


%%
%% Exported Functions
%%
-export([startReadProcess/1,startWriteProcess/1,dieP/0]).

%%
%% API Functions
%%

startReadProcess(FileID)->
    spawn(node(),fileMan,fun readProcess/1,FileID).

startWriteProcess(FileID)->
    spawn(node(),fileMan,fun writeProcess/1,FileID).
        
%%
%% Local Functions
%%

dieP()->
    receive
        _ ->
            1000 div 0
    end.

readProcess(FileID)->
    receive
        {locatechunk, FileID, ChunkIndex} ->
			io:format("asdf"),
			readProcess(FileID);
        {get_chunk,ChunkIdx}->
            todo
	end.

writeProcess(FileID)->
    receive        
        {allocate,_ClientID}->    % if append,  we shall return that very last chunk.
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
                            {error, "no data server active"};
                        true ->
                            random:seed(),
    						Select = random:uniform(SizeOfRes),
    						HostRecord = lists:nth(Select,Res),
							SelectedHost = HostRecord#hostinfo.procname
                    end;
                    
                _ ->
                    io:format("last chunk allocated!~n"),
                    allocate_this
            end;
        {register_chunk,_ClientID, ChunkUsedSize, _NodeList}->
            todo;
        {close,_ClientID}->
            todo          
    end.



%% write step 2: allocate chunk
do_allocate_chunk_o(FileID, _ClientID)-> 
    % allocate data chunk
    {_, HighTime, LowTime}=now(),
	ChunkID = <<HighTime:32, LowTime:32>>,
    
    Res = select_all_from_Table(hostinfo),  % [{}{}{}{}{}]    
    SizeOfRes = length(Res),
    
    % if hostinfo table is empty, report error to client
    if 
        % hostinfo table is empty!!! error occurs!
        (SizeOfRes =< 0) ->
            {erroe, "no data server active"};
        
        % hostinfo table is not empty
        true ->
    		random:seed(),
    		Select = random:uniform(SizeOfRes),
    		HostRecord = lists:nth(Select,Res),
			SelectedHost = HostRecord#hostinfo.procname,
    		% SelectedHost = erlang:element(1,lists:nth(Select,Res)),
    
    		% insert chunk into filemeta_s_table
    		case select_all_from_filemeta_s(FileID) of				
				[]->            
            		{error,"file does not exist"};
                
                [FileMetaS]->
                    ChunkList = FileMetaS#filemeta_s.chunklist++[ChunkID],
                    RowFileMeta = FileMetaS#filemeta_s{chunklist = ChunkList},
            		RowChunkMapping = #chunkmapping{chunkid=ChunkID, 
                                                    chunklocations=[SelectedHost]},
           			%io:format("do allocate_chunk"),
            		write_to_db(RowFileMeta),
    				write_to_db(RowChunkMapping),
        			{ok, ChunkID, [SelectedHost]}
     		end
    end.



%% write step 3: register chunk
do_register_chunk_o(FileID, _ChunkID, ChunkUsedSize, _NodeList)->
    % register chunk    
    % update filesize inf filemeta_s table
    case select_all_from_filemeta_s(FileID) of
        [] ->
            {error,"file does not exist"};
        [FileMetaS] ->
            FileSize = FileMetaS#filemeta_s.filesize + ChunkUsedSize,
            Row = FileMetaS#filemeta_s{filesize = FileSize},
            write_to_db(Row),
            {ok, []}
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


%% read step 1: open file == wirte step1
%% read step 2: get chunk for further reading
do_get_chunk_o(FileID, ChunkIdx)->
    % mock return
    % insert chunk into filemeta_s_table
    case select_all_from_filemeta(FileID) of				
		[]->
            {error,"file does not exist"};
        
		[#filemeta{chunklist = ChunkList}]->
            if 
                (length(ChunkList) =< ChunkIdx) ->
					{error, "chunkindex is larger than chunklist size"};
				true ->
					ChunkID = lists:nth(ChunkIdx+1, ChunkList),
    				case select_nodeip_from_chunkmapping(ChunkID) of
        				[] -> {error, "chunk does not exist"};
        				% get fileid sucessfull	
        				[ChunkLocations] ->
							{ok, ChunkID, ChunkLocations}
					end
			end
	end.