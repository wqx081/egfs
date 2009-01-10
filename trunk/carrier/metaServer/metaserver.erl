-module(metaserver).
-compile(export_all).
-include("metaformat.hrl").
-include("../include/egfs.hrl").
-import(lists, [reverse/1]).

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

%%% "model" methods

%% write step 1: open file
do_open(Filename, Modes, _ClientID)->
    % Modes 
    case Modes of
        r-> do_read_open(Filename, _ClientID);
        w-> do_write_open(Filename, _ClientID);
%      	a-> do_append_open(Filename, ClientID);
        _-> {error, "unkown open mode"}
    end.
 
do_read_open(Filename, _ClientID)->
    % mock return
    % get fileid from filemetaTable
    
    case select_fileid_from_filemeta(Filename) of
        [] -> {error, "filename does not exist"};
        % get fileid sucessfull	
        [FileID] ->
            {ok, FileID}
    end.
    
    %TODO: Table: clientinfo  ,     
    % insert a record
    %{ok, <<16#ff00ff00ff00ff00:64>>}.

do_write_open(Filename, _ClientID)->
    % mock return    
    %TODO: Table: filesession {fileid, client} ,mode =w
	% get fileid from filemetaTable
    case select_fileid_from_filemeta(Filename) of
		% create file id 
        [] ->		%create new file
			case select_fileid_from_filemeta_s(Filename) of				
				[]->		
					{_, HighTime, LowTime}=now(),
					FileID = <<HighTime:32, LowTime:32>>,
					% insert into shadow_filemeta
					add_filemeta_s_item(FileID,Filename),
					{ok, FileID};
				[_]->		% another client writing
					{error,"file exist in shadow table"}
			end; 
        % get fileid sucessfull
        [_] ->	 
			{error, "file exist"}
    end.

%% write step 2: allocate chunk
do_allocate_chunk(FileID, _ClientID)-> 
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
do_register_chunk(FileID, _ChunkID, ChunkUsedSize, _NodeList)->
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
do_close(FileID, _ClientID)->
    % delete client from filesession table  
	case select_all_from_filemeta_s(FileID) of
        [{filemeta_s, FileID, FileName, FileSize, ChunkList, CreateT, ModifyT, ACL}]->
            write_to_db({filemeta, FileID, FileName, FileSize, ChunkList, CreateT, ModifyT, ACL}),
            delete_from_db({filemeta_s, FileID}),
            {ok, []};
        [_] -> 
            {ok, []};
        [] ->
            {ok, []}
    end.

%% read step 1: open file == wirte step1
%% read step 2: get chunk for further reading
do_get_chunk(FileID, ChunkIdx)->
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
    detach_from_chunk_mapping(HostName).


do_dataserver_bootreport(HostRecord, ChunkList)->
     do_register_dataserver(HostRecord, ChunkList).

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
    {ok, OrphanChunkList}.

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