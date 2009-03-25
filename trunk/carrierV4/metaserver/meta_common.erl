%% Author: zyb@fit
%% Created: 2009-3-3
%% Description: TODO: Add description to meta_common
-module(meta_common).

%%
%% Include files
%%
-include("../include/header.hrl").
%%
%% Exported Functions
%%

-compile(export_all).

%%
%% API Functions
%%


%%%%%  Part1 Naming Server

do_open(FilePathName, Mode, UserName) ->
    %% gen_server:call(?ACL_SERVER, {Mode, FilePathName, _UserName})
    
    error_logger:info_msg("[~p, ~p]:open ~p~n", [?MODULE, ?LINE,{FilePathName, Mode, UserName}]),    
    
    case get_acl() of
        true ->
            RealPath = lib_common:get_rid_of_last_slash(FilePathName),
            case meta_db:get_tag(RealPath) of
                regular ->
%%                     FileID = meta_db:get_id(ReadPath),
%%                     io:format("show FileID ~p~n",[FileID]),
%%                     io:format("call_meta_open:"),
                    case Mode of 
                        write ->
                            {error,"please use mode append"};
                        read ->
                            do_read_open(RealPath,UserName);
                        append ->
                            do_append_open(RealPath,UserName);
                        Any->
                            {error,"unknown mode: ~p~n",[Any]}
                    end;                
                directory ->
                    {error, "you are opening a dir"};
                Any -> %% create a file.
%%                     error_logger:info_msg(" tag = ~p , file not exist ~n",[Any]),
                    
                    ParentDir = filename:dirname(RealPath),
                    %%gen_server:call(?ACL_SERVER, {write, ParentDir, _UserName})
%%                     case ((Mode=:=write) and meta_db:get_tag(ParentDir)=:=dir) and get_acl() of
                    
%%                     io:format("parentdir: ~p~n",[ParentDir]),
                    %%%%%%%%%%%%TODO:
                    case (meta_db:get_tag(ParentDir)=:=directory) and (Mode=:=write) of 
                        true ->
                            do_write_open(RealPath,UserName);
                        false ->
                            {error, "parent dir does not exist or file not exist"}
                    end
            end;
        false ->
            {error, "sorry, you are not authorized to do this operation "}
    end
.

do_delete(FilePathName, _UserName)->
    error_logger:info_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE,{}]),
    %%gen_server:call(?ACL_SERVER, {delete_folder, FilePathName, _UserName})
    DeleteDir = ((meta_db:get_tag(FilePathName)=:=directory) and get_acl()),
    %%gen_server:call(?ACL_SERVER, {delete_folder, FilePathName, _UserName})
    DeleteFile = ((meta_db:get_tag(FilePathName)=:=regular) and get_acl()),
    error_logger:info_msg("DeleteDir and DeleteFile_~p,~p,~n",[DeleteDir,DeleteFile]),
    if
        DeleteDir->            
            ResList = meta_db:get_all_sub_files_byName(FilePathName),    %%ResList = {}{}...{}{}    , {} = {tag,id,name}
            %% acl.            
            call_meta_delete(list,ResList);            
        DeleteFile->
            FileID = meta_db:get_id(FilePathName),
            case check_process_byID(FileID) of
                {ok,_}->
                     meta_db:do_delete_filemeta_byID(FileID);
                {error,_}->
                    {error,FilePathName}
            end;
        true ->
            {error, "file does not exist or you are not authorized to do this operation"}
    end
.

call_meta_delete(list,FileIDList) ->
    case call_meta_check(list,FileIDList) of
        {ok,_} ->
            call_meta_do_delete(list,FileIDList);
        {error,FileID} ->
            {error, "sorry, someone is using this file, unable to delete it now .~n FileID: ~p~n",[FileID]}
    end.


%% really, do_delete,
call_meta_do_delete(list,[]) ->    
    {ok,"success"};
call_meta_do_delete(list,FileList)->   %% return ok || ThatFileID    
    [FileHead|T] = FileList,
    case call_meta_do_delete(FileHead) of
        {ok,_}->
            call_meta_do_delete(list,T);
        {error,_}->
            {error,FileHead}
    end.
call_meta_do_delete(File)->
    error_logger:info_msg("deleting file: ~p~n",[File]),
    {_Tag,FileID,_FileName} = File, 
    meta_db:do_delete_filemeta_byID(FileID).

%% checking.
call_meta_check(list,[]) ->    
    {ok,"no file conflict"};

call_meta_check(list,File)->   %% return ok || ThatFileID    
    [HFile|T] = File,
    {_Tag,FileID,_FileName} = HFile,    
    error_logger:info_msg("call_meta_CHAECK,CHECK File: ,~p~n",[HFile]),
    case check_process_byID(FileID) of
        {ok,_}->
            call_meta_check(list,T);
        {error,_}->
            {error,FileID}
    end.


%% -record(filemeta, {fileid, filename, filesize, chunklist, createT, modifyT,tag,parent}). 
%%-record(filemeta,{id,name,chunklist,parent,size,type,access,atime,mtime,ctime,mode,links,inode,uid,gid})

%%	copy a file : just add a new file record into table filemeta 
%%  @spec copy_a_file(Fullsrc,SrcUnderDst,DesDir)-> "Msg"
%%  Fullsrc : a file name, checked before this function being called
%%	SrcUnderDst: file name of that Creating
%%	DesDir : a dir name , checked , 
copy_a_file(Fullsrc,SrcUnderDst,DesDir)->
	error_logger:info_msg("[~p, ~p]: ~n", [?MODULE, ?LINE]),    
    error_logger:info_msg("Src: ~p ,Res: ~p ,Des : ~p~n",[Fullsrc,SrcUnderDst,DesDir]),
    [SrcFile] = meta_db:select_all_from_filemeta_byName(Fullsrc),
    {filemeta,_ID,_N,Chunklist,_Parent,FileSize,_Type,_,_,_MreateT,_CodifyT,_,_,_,_,_} = SrcFile#filemeta{},
    NewFileID = lib_uuid:gen(),
    ParentID = meta_db:get_id(DesDir),
    NewFileMeta = #filemeta{
                            id=NewFileID,
                            name = SrcUnderDst,
                            chunklist = Chunklist,
                            parent = ParentID,                            
                            size = FileSize,
                            type = regular,
                            access = 0,
                            atime = 0,
                            mtime = calendar:local_time(),
                            ctime = calendar:local_time(),
                            mode = 0,
                            links = 1,
                            inode = 0,
                            uid = 0,
                            gid = 0                         
                         },
%%	check before write.     
    case check_process_byName(SrcUnderDst) of
        {ok,_}->
            meta_db:write_to_db(NewFileMeta),            
            {ok,"copy finished"};
        {error,Msg} ->
            io:format("Can't copy to new location:~p , ~p.~n",[NewFileMeta,Msg]),
            {error,NewFileMeta}         
    end.


%%	copy a dir: copy sub dirs/files to Dstdir 
%%  @spec copy_a_dir(Fullsrc,SrcUnderDst,DesDir)-> "Msg"
%%  Fullsrc : a dir name, checked before this function being called
%%	SrcUnderDst: file name of that Creating dir.
%%	DesDir : a dir name , checked , 
%%	should be recursive , call itself to resolve copy sub dirs
%%	0 check and make new dir. 
%%  1 get file/dir list
%%	2 file: copy_a_file
%% 	3 dir: copy_a_dir
%%	4 TODO: mass acl and collision problems. 
copy_a_dir(Fullsrc,SrcUnderDst,DesDir) ->
	error_logger:info_msg("[~p, ~p]: ~n", [?MODULE, ?LINE]),    
    error_logger:info_msg("copying a dir.~n Src: ~p ,Res: ~p ,Des : ~p~n",[Fullsrc,SrcUnderDst,DesDir]),
    %% 1 
    ParentFileID = meta_db:get_id(Fullsrc),
    ResList = meta_db:get_direct_sub_files(ParentFileID),		%%ResList = {}{}...{}{}    , {} = {tag,id,name}
    %% 2 acl & collision
    case meta_db:select_all_from_filemeta_byName(SrcUnderDst) of
        []->
            DirID = lib_uuid:gen(),
            ParentID = meta_db:get_id(DesDir),
            meta_db:add_new_dir(DirID,SrcUnderDst,ParentID),            
            copy_file_crowd(ResList,SrcUnderDst),
            error_logger:info_msg("copy dir :~p finished~n",[Fullsrc]),
            ok;
        _Any->
            {error,"Destination Dir exist."}
    end.

copy_file_crowd([],DesDir)->
    error_logger:info_msg("end of the list. Dst: ~p~n",DesDir),    
    welcome;
copy_file_crowd(ResList,DesDir)->
    error_logger:info_msg("copying a list,."),    
    [HeadFile|T] = ResList,
    {Tag,_FileID,FileName} = HeadFile,
    case Tag of 
        regular ->
            SrcUnderDst = filename:join(DesDir,filename:basename(FileName)),            
            copy_a_file(FileName,SrcUnderDst,DesDir);
        directory ->
            SrcUnderDst = filename:join(DesDir,filename:basename(FileName)),
            copy_a_dir(FileName,SrcUnderDst,DesDir);
        Any ->
            error_logger:info_msg("error,wtf....~p,~n",[Any])
    end,
    copy_file_crowd(T,DesDir).      


do_copy(FullSrc, FullDst, _UserName)->	
    error_logger:info_msg("[~p, ~p]: ~n,In Do_Copy, src: ~p, dst: ~p~n",[?MODULE, ?LINE,FullSrc, FullDst]),
    CheckResult = check_op_type(FullSrc, FullDst),
    error_logger:info_msg("check op type res: ~p~n",[CheckResult]),
    case CheckResult of
        {ok, caseFileToDir, SrcUnderDst} ->
            copy_a_file(FullSrc,SrcUnderDst,FullDst),
            {ok,single_file_copy};        
        {ok, caseDirToDir, SrcUnderDst} ->
            copy_a_dir(FullSrc,SrcUnderDst,FullDst),
            {ok,dir_copy};        
        {error,Msg}->
            error_logger:info_msg("copy fail: ~n"),
            error_logger:info_msg(Msg++"~n"),
            {error,Msg}
    end.
%% do_copy + do_delete.
%% TODO: file modifytime?
do_move(FullSrc, FullDst, _UserName)->
	error_logger:info_msg("[~p, ~p]: ~n,In Do_Move, src: ~p, dst: ~p~n",[?MODULE, ?LINE,FullSrc, FullDst]),
    error_logger:info_msg("In Do_Move, src: ~p, dst: ~p~n",[FullSrc, FullDst]),
    CheckResult = check_op_type(FullSrc, FullDst),
    error_logger:info_msg("check op type res: ~p~n",[CheckResult]),
    %% acl as copy.   if can write , just gen new file first, then think about delete
    case CheckResult of
        {ok, caseFileToDir, SrcUnderDst} ->
            copy_a_file(FullSrc,SrcUnderDst,FullDst),
            do_delete(FullSrc, _UserName), %% with acl check in function.
            {ok,single_file_move};        
        {ok, caseDirToDir, SrcUnderDst} ->
            copy_a_dir(FullSrc,SrcUnderDst,FullDst),
            do_delete(FullSrc,_UserName),
            {ok,dir_move};        
        {error,Msg}->
            error_logger:info_msg("move fail, copy fail.: ~n"),
            error_logger:info_msg(Msg++"~n"),
            {error_movefilefail,Msg}        
    end
.


do_list(FilePathName,_UserName)->
    error_logger:info_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE,{}]),
    %    gen_server:call(?ACL_SERVER, {read, filename:dirname(FilePathName), _UserName})
    case get_acl() and (meta_db:get_tag(FilePathName)=:=directory) of
        true ->
            ParentDirID = meta_db:get_id(FilePathName),
            %%TODO:
%%             meta_db:get_direct_sub_files(ParentDirID);
			meta_db:get_order_direct_sub_files(ParentDirID);
        false ->
            {error, "not a dir or you are not authorized to do this operation "}
    end 
.

do_mkdir(PathName, _UserName)->
    error_logger:info_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE,{}]),
    case meta_db:get_tag(PathName) of
        null ->
            ParentDirName = filename:dirname(PathName),
            %%gen_server:call(?ACL_SERVER, {write, ParentDirName, _UserName})
            case get_acl()  and (meta_db:get_tag(ParentDirName)=:=directory) of
                true ->
                    meta_db:add_new_dir(lib_uuid:gen(),PathName,meta_db:get_id(ParentDirName));
                false ->
                    {error, "sorry, you are not authorized to do this operation "}
            end;
        _Any ->
            {error, "file with this name exists already"}
    end
.

do_chmod(FileName, _UserName, _UserType, _CtrlACL) ->
    error_logger:info_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE,{}]),
    case meta_db:get_tag(FileName) of
        null ->
            {error, "file not exists"};
        _Any ->
            %            gen_server:call(?ACL_SERVER, {setacl, FileName, _UserName, UserType, CtrlACL})
            get_acl()
    end
.

%% 
%% uuid_gen_N(0) ->
%%     [];
%% uuid_gen_N(N) ->
%%     lists:append([lib_uuid:gen()],uuid_gen_N(N-1))
%% .

get_acl() ->
    true
.




%% filename:dirname("/a") -> "/" ,  ("/a/")->"/a"  ,"//" ->"/" , ""->"."
%%		-> conclude: .  | File | Dir        
%% filename:basename("/") ->[] , ("/a/")->"a" ("/a")->a
%%		-> conclude: [] | File | Dir

%%@spec check_op_type(Src,Dst)-> {ok,type} ||{error,...}
check_op_type(SrcFullPath, DstFullPath) ->
    SrcUnderDst = filename:join(DstFullPath,filename:basename(SrcFullPath)),    
    SrcTag = meta_db:get_tag(SrcFullPath),
    DstTag = meta_db:get_tag(DstFullPath),
    SrcUnderDstTag = meta_db:get_tag(SrcUnderDst),
    
%%     error_logger:info_msg("SrcUnderDst:~p,SrcTag:~p,DstTag:~p,SrcUnderDstTag:~p~n",[SrcUnderDst,SrcTag,DstTag,SrcUnderDstTag]),
    
    CaseSrcequalDes = string:equal(SrcFullPath,DstFullPath),

    CaseFileToUnFile = (SrcTag=:=regular) and (DstTag=:=null),
    CaseFileToDir = (SrcTag=:=regular) and (DstTag=:=directory) and (SrcUnderDstTag=:=null),
    CaseDirToDir = (SrcTag=:=directory) and (DstTag=:=directory) and (SrcUnderDstTag=:=null) 
									and (string:rstr(DstFullPath,SrcFullPath++"/")=:=0), 
    %% TODO: copy dir /hxm to dir /hxm/copy is not allowed , but /hxm -> /hxmOtherdir is allowed, 
    																			
    CaseDirToUnDir = (SrcTag=:=directory) and (DstTag=:=null), 
    if       
        CaseSrcequalDes ->
            {error," Src = Des."};
        CaseFileToUnFile ->
            {error," rename not supported"};
        CaseFileToDir ->
            {ok, caseFileToDir, SrcUnderDst};
        CaseDirToDir ->
            {ok, caseDirToDir, SrcUnderDst};
        CaseDirToUnDir ->
            {error, "destination error"};
        true ->
            {error, "wrong input"}
    end
.
 
%%@spec check_op_type(Src,Dst)-> {ok,type} ||{error,...}
%% check_op_type_hiatusV(SrcFullPath, DstFullPath) ->
%%     SrcUnderDst = filename:join(DstFullPath,filename:basename(SrcFullPath)),
%%     DstParent = filename:dirname(DstFullPath),
%%     
%%     SrcTag = meta_db:get_tag(SrcFullPath),
%%     DstTag = meta_db:get_tag(DstFullPath),
%%     SrcUnderDstTag = meta_db:get_tag(SrcUnderDst),
%%     DstParentTag = meta_db:get_tag(DstParent),
%% 
%%     CaseFileToUnFile = (SrcTag=:=file) and (DstTag=:=null) and (DstParentTag=:=dir),
%%     CaseFileToDir = (SrcTag=:=file) and (DstTag=:=dir) and (SrcUnderDstTag=:=null) and (string:rstr(DstFullPath,SrcFullPath)=:=0),
%%     CaseDirToDir = (SrcTag=:=dir) and (DstTag=:=dir) and (SrcUnderDstTag=:=null) and (string:rstr(DstFullPath,SrcFullPath)=:=0),
%%     CaseDirToUnDir = (SrcTag=:=dir) and (DstTag=:=null) and (DstParentTag=:=dir),
%%     if
%%         CaseFileToUnFile ->
%%             {ok, caseFileToUnFile, DstParent, DstParent};
%%         CaseFileToDir ->
%%             {ok, caseFileToDir, DstFullPath, DstFullPath};
%%         CaseDirToDir ->
%%             {ok, caseDirToDir, DstFullPath, SrcUnderDst};
%%         CaseDirToUnDir ->
%%             {ok, caseDirToUnDir, DstParent, DstFullPath};
%%         true ->
%%             {error, "wrong input"}
%%     end
%% .

%%%%%%%%%%%%%%%%


%% @spec check_process_byName(ID) -> {ok,FileName} | {error,"output"} 
check_process_byID(FileID)->
    FileName = meta_db:get_name(FileID),
    check_process_byName(FileName).


%% @spec check_process_byName(Name) -> {ok,FileName} | {error,"output"}  
check_process_byName(FileName) ->
    WA = lib_common:generate_processname(FileName,write),
    RA = lib_common:generate_processname(FileName,read),
%%     error_logger:info_msg("Write processname: ~p~n",[WA]),
%%     error_logger:info_msg("Read processname: ~p~n",[RA]),
    case whereis(WA) of
        undefined->
            case whereis(RA) of
                undefined->
                    {ok,FileName};
                _->
                    {error,"someone reading this file."}
            end;
        _->
            {error,"someone writing this file."}
    end.    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%meta_server

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5
%%from old meta_server ;   handle these function.

%% do_file_open(FileName,Mod,UserName)-> 
%%     case Mod of
%%         read ->            
%%             do_read_open(FileName,UserName);
%%         write ->
%%             do_write_open(FileName,UserName);
%%         append->
%%             do_append_open(FileName,UserName);
%%         _->
%%             {error,"unkown open mode"}
%%     end.


%% checked, no file exist , go ahead and write, no one reading or appending
do_write_open(FileName,UserName)->    
    error_logger:info_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE,{}]),
    ProcessName = lib_common:generate_processname(FileName,write),
%%    io:format("FileName: ~p~n;ProcessName~p~n",[FileName,ProcessName]),
    case whereis(ProcessName) of
        undefined -> % no meta worker , create one worker to server this writing request.
%%             io:format("C...~n"),
			case meta_db:get_id(FileName) of                
				null ->
%%                    io:format("A...~n"),
					% if the target file is not exist, then generate a new fileid. 
                    FileID = lib_uuid:gen(),
%%                     io:format("A1...~n"),
				    FileRecord = #filemeta{	id=FileID, 
											name=FileName, 
											size=0, 
											chunklist=[], 
				                 			ctime=calendar:local_time(), 
											mtime=calendar:local_time()
                                            },
%%                     io:format("A2...~n"),
					{ok, MetaWorkerPid}=gen_server:start({local,ProcessName}, meta_worker, [FileRecord, write,UserName], []),
					{ok, FileID, 0, [], MetaWorkerPid};	  
				Any ->
%%                     io:format("B...~n"),
					{error, "Cant Write! The same file name has existed in database.FileID:~p,FileName:~p~n",[Any,FileName]}
			end;
        _Pid->			% pid exist, write error
            {error, "other client is writing the same file."}  
    end.

%%can't be mode write
%% don't care someone reading,
%% also go ahead and do append,
do_append_open(FileName,UserName) ->
    error_logger:info_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE,{}]),
    ProcessName = lib_common:generate_processname(FileName,append),
    case whereis(ProcessName) of
        undefined->
            case meta_db:select_all_from_filemeta_byName(FileName) of
                [] ->
                    {error,"NO TARGET FILE EXSIT.~p~n",[FileName]};
                [Meta]->
                    {ok, MetaWorkerPid}=gen_server:start({local,ProcessName}, meta_worker, [Meta, append,UserName], []),
                    case Meta#filemeta.chunklist of 
                        []->
                            {ok,Meta#filemeta.id,Meta#filemeta.size,[],MetaWorkerPid};
                        _Any ->
                            {ok,Meta#filemeta.id,Meta#filemeta.size,lists:last(Meta#filemeta.chunklist),MetaWorkerPid}
                    end
            		%%{ ok,id,size,lastchunkid,MetaWorkerPid}
            end;
        _AnyPid->
            {error, "other client is appending the same file."}
    end.

%%go ahead and read, if file exist(means no one writing, maybe someone appending)  
do_read_open(FileName,UserName)->		
    error_logger:info_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE,{}]),
%%     error_logger:info_msg("in do_read_open~n"),
    ProcessName = lib_common:generate_processname(FileName,read),
    case whereis(ProcessName) of
        undefined ->		% no meta worker , create one worker to server this writing request.
            case meta_db:select_all_from_filemeta_byName(FileName) of			
				[] ->
					% if the target file is not exist, then report error .		
					{error, "the target file is not exist."};  
				[FileMeta] ->	 %%%%%%%%%%%%%%%%%%%%% wait for modifying 
					{ok, MetaWorkerPid}=gen_server:start({local,ProcessName}, meta_worker, [FileMeta,read,UserName], []),
					{ok, FileMeta#filemeta.id, FileMeta#filemeta.size, FileMeta#filemeta.chunklist, MetaWorkerPid}
			end;
        Pid->			% pid exist, set this pid process as the meta worker
        	FileMeta = gen_server:call(Pid, {getfileinfo,FileName}),
       		{ok, FileMeta#filemeta.id, FileMeta#filemeta.size, FileMeta#filemeta.chunklist, Pid}
   end.

%%
%% 
%% host drop.
%% update chunkmapping and delete chunks
%% arg -  that host
%% return ---
do_detach_one_host(HostName)->
    meta_db:detach_from_chunk_mapping(HostName). %metaDB fun


do_dataserver_bootreport(HostRecord, ChunkList)->    
    meta_db:do_register_dataserver(HostRecord, ChunkList). %metaDB fun

%% 
%%delete 
%% do_delete(FileName, _From)->
%%     case meta_db:select_fileid_from_filemeta(FileName) of
%%         [] -> {error, "filename does not exist"};
%%         % get fileid sucessfull	
%%         [FileID] ->
%%             meta_db:do_delete_filemeta(FileID)
%%     end.

%%
do_register_replica(ChunkID,Host) ->
    error_logger:info_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE,{}]),
    error_logger:info_msg("in do_register_replica,~p,~p~n",[ChunkID,Host]),
    case meta_db:select_item_from_chunkmapping_id(ChunkID) of
        []->            
            meta_db:write_to_db(#chunkmapping{chunkid = ChunkID,chunklocations = [Host]});
        [X]->
            case lists:member(Host,X#chunkmapping.chunklocations) of
                true->
                    {ok,"bie nao"};            
                 false->
                   New = X#chunkmapping{chunklocations = X#chunkmapping.chunklocations++[Host]},
                   meta_db:write_to_db(New)
            end
    end.

%% 
%%
%% do_collect_orphanchunk(HostProcName)->
%%     % get orphanchunk from orphanchunk table
%%     OrphanChunkList = meta_db:select_chunkid_from_orphanchunk(HostProcName),
%%     % delete notified orphanchunk from orphanchunk table
%%     meta_db:do_delete_orphanchunk_byhost(HostProcName),
%%     OrphanChunkList.

%% 
%% debug 
%% 
do_debug(Arg) ->
    error_logger:info_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE,{}]),    
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
            io:format("u wna die ,i let u die.~n")
%%             1000 div 0;
            ;
        _ ->
            io:format("ohter~n")
    end.

do_showWorker(FileName,Mod)->
    error_logger:info_msg("[~p, ~p]: ~p~n", [?MODULE, ?LINE,{}]),
    ProcessName = lib_common:generate_processname(FileName,Mod),
    case whereis(ProcessName) of
        undefined ->		% no meta worker , create one worker to server this writing request.
            io:format(" no this file worker exist."),
            {result,nofileexist};
        Pid->			% pid exist, set this pid process as the meta worker
        	{state,gen_server:call(Pid, {debug})}
   end.


%% 
%% seperate_file_dir([]) ->
%%     [[],[],[],[]];
%% seperate_file_dir(FileList) ->
%%     [{Tag, ID,Name}|Left] = FileList,
%%     [LeftFiles,LeftDirs,LeftFileNames,LeftDirNames] = seperate_file_dir(Left),
%%     case Tag of
%%         regular ->
%%             [lists:append([ID],LeftFiles),LeftDirs,lists:append([Name],LeftFileNames),LeftDirNames];
%%         directory ->
%%             [LeftFiles,lists:append([ID],LeftDirs),LeftFileNames,lists:append([Name],LeftDirNames)]
%%     end
%% .

    
