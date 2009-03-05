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

do_open(FilePathName, Mode, _UserName) ->
    %% gen_server:call(?ACL_SERVER, {Mode, FilePathName, _UserName})
    case get_acl() of
        true ->
            case meta_db:get_tag(FilePathName) of
                file ->
                    FileID = meta_db:get_id(FilePathName),
                    io:format("show FileID ~p~n",[FileID]),
                    io:format("call_meta_open:"),
                    do_file_open(FilePathName,Mode);
                
                dir ->
                    {error, "you are opening a dir"};
                _Any ->                    
                    ParentDir = filename:dirname(FilePathName),
                    %%gen_server:call(?ACL_SERVER, {write, ParentDir, _UserName})
%%                     case ((Mode=:=write) and meta_db:get_tag(ParentDir)=:=dir) and get_acl() of
                    
                    io:format("parentdir: ~p~n",[ParentDir]),
                    %%%%%%%%%%%%TODO:
                    case true  of 
                        true ->
                            do_file_open(FilePathName, Mode);
                        false ->
                            {error, "parent dir does not exist"}
                    end
            end;
        false ->
            {error, "sorry, you are not authorized to do this operation "}
    end
.

do_delete(FilePathName, _UserName)->
    %%gen_server:call(?ACL_SERVER, {delete_folder, FilePathName, _UserName})
    DeleteDir = ((meta_db:get_tag(FilePathName)=:=dir) and get_acl()),
    %%gen_server:call(?ACL_SERVER, {delete_folder, FilePathName, _UserName})
    DeleteFile = ((meta_db:get_tag(FilePathName)=:=file) and get_acl()),
    if
        DeleteDir or DeleteFile->
            FileID = meta_db:get_id(FilePathName),
            [FileIDList, DirIDList,FileNameList,DirNameList] = meta_db:get_all_sub_files(FileID),
            AllDirIDList = lists:append(DirIDList,[FileID]),
            case call_meta_delete(list, FileNameList) of
                {ok,_} ->
                    meta_db:delete_rows(FileIDList),
                    meta_db:delete_rows(AllDirIDList),
                    %%gen_server:call(?ACL_SERVER, {delete_aclrecord, FilePathName});
                    get_acl();
                {error,FileID}->
                    {error, "sorry, someone is using this file, unable to delete it now .~n FileID: ~p~n",[FileID]}
            end;
        true ->
            {error, "file does not exist or you are not authorized to do this operation"}
    end
.


do_copy(FullSrc, FullDst, _UserName)->
    CheckResult = check_op_type(FullSrc, FullDst),    
    case CheckResult of
        {ok,Case,_DirToAcl,RealDst} ->
            %% 冗余，待整理                       
            %%gen_server:call(?ACL_SERVER, {copyfolder, FullSrc, DirToAcl,_UserName})
            CopyDir = ((Case=:=caseDirToDir) or (Case=:=caseDirToUnDir))
            and get_acl(),
            %%gen_server:call(?ACL_SERVER, {copyfolder, FullSrc, DirToAcl,_UserName})
            CopyFile = ((Case=:=caseFileToDir) or (Case=:=caseFileToUnFile))
            and get_acl(),
            if
                CopyDir ->
                    meta_db:add_new_dir(lib_uuid:gen(),RealDst,meta_db:get_id(filename:dirname(RealDst))),
                    SrcFileID = meta_db:get_id(FullSrc),
                    [SrcFileIDList, SrcDirIDList] = meta_db:get_all_sub_files(SrcFileID),
                    DstFileIDList = uuid_gen_N(length(SrcFileIDList)),
                    case call_meta_copy(list, SrcFileIDList, DstFileIDList) of
                        true ->
                            DstDirIDList = uuid_gen_N(length(SrcDirIDList)),
                            meta_db:add_copyed_files(SrcDirIDList,DstDirIDList,FullSrc,RealDst),
                            meta_db:add_copyed_files(SrcFileIDList,DstFileIDList,FullSrc,RealDst);
                        false ->
                            {error, "error when copying"}
                    end;
                CopyFile ->
                    DstFileID = lib_uuid:gen(),
                    SrcFileID = meta_db:get_id(FullSrc),
                    case call_meta_copy(list, [SrcFileID], [DstFileID]) of
                        true when (Case=:=caseFileToUnFile) ->
                            {CT,MT} = meta_db:get_time(SrcFileID),
                            meta_db:add_one_row(DstFileID,FullDst,CT,MT,file,meta_db:get_id(filename:dirname(FullDst)));
                        true when (Case=:=caseFileToDir) ->
                            meta_db:add_copyed_files([SrcFileID],[DstFileID],filename:dirname(FullSrc),RealDst);
                        false ->
                            {error, "error when copying"}
                    end;
                true ->
                    {error, "sorry, you are not authorized to do this operation "}
            end;
        {error, _} ->
            {error, "wrong input"}
    end
.



do_move(FullSrc, FullDst, _UserName)->
    CheckResult = check_op_type(FullSrc, FullDst),
    case CheckResult of
        {ok, _,_,_} ->
            %% 冗余，待整理
            {ok,Case,_DirToAcl,RealDst} = CheckResult,
            %%gen_server:call(?ACL_SERVER, {copyfoder, FullSrc, DirToAcl, _UserName}) and gen_server:call(?ACL_SERVER, {delete, FullSrc, _UserName})
            CopyDirAcl = get_acl(),
            %%gen_server:call(?ACL_SERVER, {copyfile, FullSrc, DirToAcl, _UserName}) and gen_server:call(?ACL_SERVER, {delete, FullSrc, _UserName})
            CopyFileAcl = get_acl(),
            case Case of
                caseDirToUnDir when CopyDirAcl->
                    SrcFileID = meta_db:get_id(FullSrc),
                    {CT,MT} = meta_db:get_time(SrcFileID),
                    meta_db:add_one_row(SrcFileID,RealDst,CT,MT,dir,meta_db:get_id(filename:dirname(RealDst))),
                    [SrcFileIDList, SrcDirIDList] = meta_db:get_all_sub_files(SrcFileID),
                    meta_db:add_moved_files(SrcDirIDList,FullSrc,RealDst),
                    meta_db:add_moved_files(SrcFileIDList,FullSrc,RealDst),
                    %                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                    get_acl();
                caseDirToDir when CopyDirAcl->
                    SrcFileID = meta_db:get_id(FullSrc),
                    {CT,MT} = meta_db:get_time(SrcFileID),
                    meta_db:add_one_row(SrcFileID,RealDst,CT,MT,dir,meta_db:get_id(filename:dirname(RealDst))),
                    [SrcFileIDList, SrcDirIDList] = meta_db:get_all_sub_files(SrcFileID),
                    meta_db:add_moved_files(SrcDirIDList,FullSrc,RealDst),
                    meta_db:add_moved_files(SrcFileIDList,FullSrc,RealDst),
                    %                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                    get_acl();
                caseFileToUnFile when CopyFileAcl->
                    SrcFileID = meta_db:get_id(FullSrc),
                    {CT,MT} = meta_db:get_time(SrcFileID),
                    meta_db:add_one_row(SrcFileID,FullDst,CT,MT,file,meta_db:get_id(filename:dirname(FullDst))),
                    %                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                    get_acl();
                caseFileToDir when CopyFileAcl->
                    SrcFileID = meta_db:get_id(FullSrc),
                    meta_db:add_moved_files([SrcFileID],filename:dirname(FullSrc),RealDst),
                    %                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                    get_acl();
                _Any ->
                    {error, "sorry, you are not authorized to do this operation "}
            end;
        {error, _} ->
            {error, "wrong input"}
    end
.


do_list(FilePathName,_UserName)->
    %    gen_server:call(?ACL_SERVER, {read, filename:dirname(FilePathName), _UserName})
    case get_acl() and (meta_db:get_tag(FilePathName)=:=dir) of
        true ->
            ParentDirID = meta_db:get_id(FilePathName),
            []meta_db:get_direct_sub_files(ParentDirID);
        false ->
            {error, "not a dir or you are not authorized to do this operation "}
    end 
.

do_mkdir(PathName, _UserName)->
    case meta_db:get_tag(PathName) of
        null ->
            ParentDirName = filename:dirname(PathName),
            %%gen_server:call(?ACL_SERVER, {write, ParentDirName, _UserName})
            case get_acl()  and (meta_db:get_tag(ParentDirName)=:=dir) of
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
    case meta_db:get_tag(FileName) of
        null ->
            {error, "file not exists"};
        _Any ->
            %            gen_server:call(?ACL_SERVER, {setacl, FileName, _UserName, UserType, CtrlACL})
            get_acl()
    end
.



uuid_gen_N(0) ->
    [];
uuid_gen_N(N) ->
    lists:append([lib_uuid:gen()],uuid_gen_N(N-1))
.
get_acl() ->
    true
.


%%@spec check_op_type(Src,Dst)-> {ok,type} ||{error,...}
check_op_type(SrcFullPath, DstFullPath) ->
    SrcUnderDst = filename:join(DstFullPath,filename:basename(SrcFullPath)),
    DstParent = filename:dirname(DstFullPath),

    SrcTag = meta_db:get_tag(SrcFullPath),
    DstTag = meta_db:get_tag(DstFullPath),
    SrcUnderDstTag = meta_db:get_tag(SrcUnderDst),
    DstParentTag = meta_db:get_tag(DstParent),

    CaseFileToUnFile = (SrcTag=:=file) and (DstTag=:=null) and (DstParentTag=:=dir),
    CaseFileToDir = (SrcTag=:=file) and (DstTag=:=dir) and (SrcUnderDstTag=:=null) and (string:rstr(DstFullPath,SrcFullPath)=:=0),
    CaseDirToDir = (SrcTag=:=dir) and (DstTag=:=dir) and (SrcUnderDstTag=:=null) and (string:rstr(DstFullPath,SrcFullPath)=:=0),
    CaseDirToUnDir = (SrcTag=:=dir) and (DstTag=:=null) and (DstParentTag=:=dir),
    if
        CaseFileToUnFile ->
            {ok, caseFileToUnFile, DstParent, DstParent};
        CaseFileToDir ->
            {ok, caseFileToDir, DstFullPath, DstFullPath};
        CaseDirToDir ->
            {ok, caseDirToDir, DstFullPath, SrcUnderDst};
        CaseDirToUnDir ->
            {ok, caseDirToUnDir, DstParent, DstFullPath};
        true ->
            {error, "wrong input"}
    end
.


%%%%%%%%%%%%%%%%

call_meta_delete(list,FileIDList) ->
    case call_meta_check(list,FileIDList) of
        {ok,_} ->
            call_meta_do_delete(list,FileIDList);
        {error,FileID} ->
            {error,FileID}
    end.


call_meta_do_delete(list,[]) ->    
    {ok,"success"};
call_meta_do_delete(list,FileIDList)->   %% return ok || ThatFileID    
    [FileID|T] = FileIDList,
    case call_meta_do_delete(FileID) of
        {ok,_}->
            call_meta_do_delete(list,T);
        {error,_}->
            {error,FileID}
    end.

call_meta_do_delete(FileID)->
    meta_db:do_delete_filemeta(FileID).





call_meta_copy(list,_S,_D)->
    %%TODO.
    todo.

call_meta_copy(_FileID) ->
 %%TODO.
%%     gen_server:call(?CHUNK_SERVER,{chunkCopy,[From],[To1,To2]}),
    
    {ok, <<1:64>>}
.

call_meta_check(list,[]) ->    
    {ok,"no file conflict"};
call_meta_check(list,FileIDList)->   %% return ok || ThatFileID
    io:format("aaa,~p~n",[FileIDList]),
    [FileID|T] = FileIDList,
    case call_meta_check(FileID) of
        {ok,_}->
            call_meta_check(list,T);
        {error,_}->
            {error,FileID}
    end.

call_meta_check(FileID)->
    FileName = meta_db:get_name(FileID),
    WA = lib_common:generate_processname(FileName,write),
    RA = lib_common:generate_processname(FileName,read),
    
    case whereis(WA) of
        undefined->
            case whereis(RA) of
                undefined->
                    {ok,FileID};
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

do_file_open(FileName,Mod)-> 
    case Mod of
        read ->            
            do_read_open(FileName);
        write ->
            do_write_open(FileName);
        _->
            {error,"unkown open mode"}
    end.

do_write_open(FileName)->    
    ProcessName = lib_common:generate_processname(FileName,w),
    io:format("FileName: ~p~n;ProcessName~p~n",[FileName,ProcessName]),
    case whereis(ProcessName) of
        undefined -> % no meta worker , create one worker to server this writing request.
            io:format("C...~n"),
			case meta_db:get_id_from_dirmeta(FileName) of                
				[] ->
                    io:format("A...~n"),
					% if the target file is not exist, then generate a new fileid. 
                    FileID = lib_uuid:gen(),
                    io:format("A1...~n"),
				    FileRecord = #filemeta{	fileid=FileID, 
											filename=FileName, 
											filesize=0, 
											chunklist=[], 
				                 			createT=calendar:local_time(), 
											modifyT=calendar:local_time()
                                            },
                    io:format("A2...~n"),
					{ok, MetaWorkerPid}=gen_server:start({local,ProcessName}, meta_worker, [FileRecord, write], []),
					{ok, FileID, 0, [], MetaWorkerPid};	  
				[_FileMeta] ->
                    io:format("B...~n"),
					{error, "Cant Write! The same file name has existed in database."}
			end;
        _Pid->			% pid exist, write error
            {error, "other client is writing the same file."}  
    end.

do_read_open(FileName)->
    ProcessName = lib_common:generate_processname(FileName,r),
    case whereis(ProcessName) of
        undefined ->		% no meta worker , create one worker to server this writing request.
            case meta_db:select_items_from_dirandfile(FileName) of			
				[] ->
					% if the target file is not exist, then report error .		
					{error, "the target file is not exist."};  
				[FileMeta] ->	 %%%%%%%%%%%%%%%%%%%%% wait for modifying 
					{ok, MetaWorkerPid}=gen_server:start({local,ProcessName}, meta_worker, [FileMeta,read], []),
					{ok, FileMeta#filemeta.fileid, FileMeta#filemeta.filesize, FileMeta#filemeta.chunklist, MetaWorkerPid}
			end;
        Pid->			% pid exist, set this pid process as the meta worker
        	FileMeta = gen_server:call(Pid, {getfileinfo,FileName}),
       		{ok, FileMeta#filemeta.fileid, FileMeta#filemeta.filesize, FileMeta#filemeta.chunklist, Pid}
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
    erlang:monitor_node(HostRecord#hostinfo.hostname,true,[]), %% monitor_node.
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
    case meta_db:select_item_from_chunkmapping_id(ChunkID) of
        []->            
            meta_db:write_to_db(#chunkmapping{chunkid = ChunkID,chunklocations = [Host]});
        [X]->
            New = X#chunkmapping{chunklocations = X#chunkmapping.chunklocations++[Host]},
            meta_db:write_to_db(New)
    end.

%% 
%%
do_collect_orphanchunk(HostProcName)->
    % get orphanchunk from orphanchunk table
    OrphanChunkList = meta_db:select_chunkid_from_orphanchunk(HostProcName),
    % delete notified orphanchunk from orphanchunk table
    meta_db:do_delete_orphanchunk_byhost(HostProcName),
    OrphanChunkList.

do_register_heartbeat(_HostInfoRec)->
    %%TODO:
    
    {ok,"heartbeat Ok"}
    .
%%     
%%     Res = select_from_hostinfo(HostInfoRec#hostinfo.hostname),
%%     Host = HostInfoRec#hostinfo.host,
%%     case Res of
%%         [{hostinfo,_Proc,Host,_TotalSpace,_FreeSpace,_Health}]->
%%             {_,TimeTick,_} = now(),
%%             NewInfo = HostInfoRec#hostinfo{health = {TimeTick,10}}, %%   ?INIT_NODE_HEALTH = 10 , TODO, 
%%             write_to_db(NewInfo),
%%             {ok,"heartbeat ok"};                
%%         _-> {error,"chunk does not exist"}
%%     end.

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
            io:format("u wna die ,i let u die.~n")
%%             1000 div 0;
            ;
        _ ->
            io:format("ohter~n")
    end.

do_showWorker(FileName,EasyMod)->
    ProcessName = lib_common:generate_processname(FileName,EasyMod),
    case whereis(ProcessName) of
        undefined ->		% no meta worker , create one worker to server this writing request.
            io:format(" no this file worker exist."),
            {result,nofileexist};
        Pid->			% pid exist, set this pid process as the meta worker
        	{state,gen_server:call(Pid, {debug})}
   end.

    
