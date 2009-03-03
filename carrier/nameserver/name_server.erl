-module(name_server).

-export([start/0,stop/0,terminate/2]).
-export([init/1, handle_call/3, handle_cast/2,handle_info/2]).
%-define(ROOT_DIR, "/home/mere/myroot").
%-define(ROOT_DIR, "/").
-define(ROOT_DIR, "/").
-define(ACL_SERVER, {global, acl_server}).
-define(NAME_SERVER, {global, name_server}).
-compile(export_all).

init(_Arg) ->
    process_flag(trap_exit, true),
    io:format("name server starting~n"),
    {ok, []}.

start() ->
    gen_server:start_link(?NAME_SERVER, name_server, [], []).

stop() ->
    gen_server:cast(?NAME_SERVER, stop).

terminate(_Reason, _State) ->
    io:format("name server terminating~n").


%%"name server" methods
%% 1: open file
%% FilePathName->string().
%% Mode -> w|r|a
%% UserToken -> <<integer():64>>
%% return -> {ok, FileID} | {error, []}
handle_call({open, FilePathName, Mode, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_open, FileName:~p,Mode:~p,Token:~p~n",[FilePathName,Mode,UserToken]),
    Reply = do_open(FilePathName, Mode, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 2: delete file
%% FilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({delete, FilePathName, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_delete, FilePathName:~p,UserToken:~p~n",[FilePathName,UserToken]),
    Reply = do_delete(FilePathName, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 3: copy file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({copy, SrcFilePathName, DstFilePathName, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_copy, SrcFilePathName:~p, DstFilePathName:~p, UserToken:~p~n",[SrcFilePathName, DstFilePathName, UserToken]),
    Reply = do_copy(SrcFilePathName, DstFilePathName, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 4: move file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({move, SrcFilePathName, DstFilePathName, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_move, SrcFilePathName:~p, DstFilePathName:~p, UserToken:~p~n",[SrcFilePathName, DstFilePathName, UserToken]),
    Reply = do_move(SrcFilePathName, DstFilePathName, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 5: list file/directory
%% FilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({list, FilePathName, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_list, FilePathName:~p,UserToken:~p~n",[FilePathName, UserToken]),
    Reply = do_list(FilePathName, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 6: mkdir file/directory
%% PathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({mkdir, PathName, UserToken}, {_From, _}, State) ->
    io:format("inside handle_call_mkdir, PathName:~p,UserToken:~p~n",[PathName, UserToken]),
    Reply = do_mkdir(PathName, UserToken),
    {reply, Reply, State};

%%"name server" methods
%% 7: change mod
%% FileName->string().
%% UserName->string().
%% UserType->user/group
%%CtrlACL->0~7
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({chmod, FileName, UserName, UserType, CtrlACL}, {_From, _}, State) ->
    io:format("inside handle_call_chmod, FileName:~p~n UserName:~p~n UserType:~p~n CtrlACL:~p~n",[FileName, UserName, UserType, CtrlACL]),
    Reply = do_chmod(FileName, UserName, UserType, CtrlACL),
    {reply, Reply, State};

%%"name server" methods
%% 8: set root
%% RootDir->string().
%% return -> {ok, []} | {error, []}
handle_call({setroot, RootDir}, {_From, _}, State) ->
    io:format("inside handle_call_set_root, RootDir:~p~n",[RootDir]),
    Reply = do_setroot(RootDir),
    {reply, Reply, State};

handle_call(_, {_From, _}, State)->
    io:format("inside handle_call_error~n"),
	Reply = {error, "undefined handler"},
    {reply, Reply, State}.

handle_cast(stop, State) ->
    io:format("name server stopping~n"),
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

open(FilePathName, Mode, UserToken) ->
    gen_server:call(?NAME_SERVER, {open, FilePathName, Mode, UserToken}).

delete(FilePathName, UserToken)->
    gen_server:call(?NAME_SERVER, {delete, FilePathName, UserToken}).

copy(SrcFilePathName, DstFilePathName, UserToken)->
    gen_server:call(?NAME_SERVER, {copy, SrcFilePathName, DstFilePathName, UserToken}).

move(SrcFilePathName, DstFilePathName, UserToken)->
    gen_server:call(?NAME_SERVER, {move, SrcFilePathName, DstFilePathName, UserToken}).

list(FilePathName, UserToken)->
    gen_server:call(?NAME_SERVER, {list, FilePathName, UserToken}).

mkdir(PathName, UserToken)->
    gen_server:call(?NAME_SERVER, {mkdir, PathName, UserToken}).

chmod(FileName, UserName, UserType, CtrlACL)->
    gen_server:call(?NAME_SERVER, {chmod, FileName, UserName, UserType, CtrlACL}).

setroot(RootDir) ->
    gen_server:call(?NAME_SERVER, {setroot, RootDir}).


do_open(FilePathName, Mode, UserName) ->
    RealFilePathName = get_real_path(FilePathName),
    case gen_server:call(?ACL_SERVER, {Mode, RealFilePathName, UserName}) of
        true ->
            case queryDB:get_tag(RealFilePathName) of
                file ->
                    FileID = queryDB:get_id(RealFilePathName),
                    call_meta_open(FileID, Mode);
                dir ->
                    {error, "you are opening a dir"};
                _Any ->
                    ParentDir = filename:dirname(RealFilePathName),
                    case (queryDB:get_tag(ParentDir)=:=dir) and gen_server:call(?ACL_SERVER, {write, ParentDir, UserName}) of
                        true ->
                            FileID = lib_uuid:uuid_gen(),
                            case call_meta_open(FileID, write) of
                                true ->
                                    queryDB:add_new_file(FileID,RealFilePathName,queryDB:get_id(ParentDir));
                                false ->
                                    {error, "unable to create this file"}
                            end;
                        false ->
                            {error, "parent dir does not exist"}
                    end
            end;
        false ->
            {error, "sorry, you are not authorized to do this operation "}
    end
.

do_delete(FilePathName, UserName)->
    RealFilePath=get_real_path(FilePathName),
    DeleteDir = ((queryDB:get_tag(RealFilePath)=:=dir) and gen_server:call(?ACL_SERVER, {delete_folder, RealFilePath, UserName})),
    DeleteFile = ((queryDB:get_tag(RealFilePath)=:=file) and gen_server:call(?ACL_SERVER, {delete_folder, RealFilePath, UserName})),
    if
        DeleteDir or DeleteFile->
            FileID = queryDB:get_id(RealFilePath),
            [FileIDList, DirIDList] = queryDB:get_all_sub_files(FileID),
            AllDirIDList = lists:append(DirIDList,[FileID]),
            case call_meta_delete(list, FileIDList) of
                true ->
                    queryDB:delete_rows(FileIDList),
                    queryDB:delete_rows(AllDirIDList),
                    gen_server:call(?ACL_SERVER, {delete_aclrecord, RealFilePath});
                false ->
                    {error, "sorry, someone is using this file, unable to delete it now"}
            end;
        true ->
            {error, "file does not exist or you are not authorized to do this operation"}
    end
.
do_copy(Src, Dst, UserName)->
    FullSrc = get_real_path(Src),
    FullDst = get_real_path(Dst),
    CheckResult = check_op_type(FullSrc, FullDst),    
    case CheckResult of
        {ok, _,_,_} ->
            %% 冗余，待整理
            {ok,Case,DirToAcl,RealDst} = CheckResult,            
            CopyDir = ((Case=:=caseDirToDir) or (Case=:=caseDirToUnDir))
            and gen_server:call(?ACL_SERVER, {copyfolder, FullSrc, DirToAcl,UserName}),
            CopyFile = ((Case=:=caseFileToDir) or (Case=:=caseFileToUnFile))
            and gen_server:call(?ACL_SERVER, {copyfile, FullSrc, DirToAcl,UserName}),
            if
                CopyDir ->
                    queryDB:add_new_dir(lib_uuid:uuid_gen(),RealDst,queryDB:get_id(filename:dirname(RealDst))),
                    SrcFileID = queryDB:get_id(FullSrc),
                    [SrcFileIDList, SrcDirIDList] = queryDB:get_all_sub_files(SrcFileID),
                    DstFileIDList = uuid_gen_N(length(SrcFileIDList)),
                    case call_meta_copy(list, SrcFileIDList, DstFileIDList) of
                        true ->
                            DstDirIDList = uuid_gen_N(length(SrcDirIDList)),
                            queryDB:add_copyed_files(SrcDirIDList,DstDirIDList,FullSrc,RealDst),
                            queryDB:add_copyed_files(SrcFileIDList,DstFileIDList,FullSrc,RealDst);
                        false ->
                            {error, "error when copying"}
                    end;
                CopyFile ->
                    DstFileID = lib_uuid:uuid_gen(),
                    SrcFileID = queryDB:get_id(FullSrc),
                    case call_meta_copy(list, [SrcFileID], [DstFileID]) of
                        true when (Case=:=caseFileToUnFile) ->
                            {CT,MT} = queryDB:get_time(SrcFileID),
                            queryDB:add_one_row(DstFileID,FullDst,CT,MT,file,queryDB:get_id(filename:dirname(FullDst)));
                        true when (Case=:=caseFileToDir) ->
                            queryDB:add_copyed_files([SrcFileID],[DstFileID],filename:dirname(FullSrc),RealDst);
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
do_move(Src, Dst, UserName)->
    FullSrc = get_real_path(Src),
    FullDst = get_real_path(Dst),
    CheckResult = check_op_type(FullSrc, FullDst),
    case CheckResult of
        {ok, _,_,_} ->
            %% 冗余，待整理
            {ok,Case,DirToAcl,RealDst} = CheckResult,
            CopyDirAcl = gen_server:call(?ACL_SERVER, {copyfoder, FullSrc, DirToAcl, UserName}) and gen_server:call(?ACL_SERVER, {delete, FullSrc, UserName}),
            CopyFileAcl = gen_server:call(?ACL_SERVER, {copyfile, FullSrc, DirToAcl, UserName}) and gen_server:call(?ACL_SERVER, {delete, FullSrc, UserName}),
            case Case of
                caseDirToUnDir when CopyDirAcl->
                    SrcFileID = queryDB:get_id(FullSrc),
                    {CT,MT} = queryDB:get_time(SrcFileID),
                    queryDB:add_one_row(SrcFileID,RealDst,CT,MT,dir,queryDB:get_id(filename:dirname(RealDst))),
                    [SrcFileIDList, SrcDirIDList] = queryDB:get_all_sub_files(SrcFileID),
                    queryDB:add_moved_files(SrcDirIDList,FullSrc,RealDst),
                    queryDB:add_moved_files(SrcFileIDList,FullSrc,RealDst),
                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                caseDirToDir when CopyDirAcl->
                    SrcFileID = queryDB:get_id(FullSrc),
                    {CT,MT} = queryDB:get_time(SrcFileID),
                    queryDB:add_one_row(SrcFileID,RealDst,CT,MT,dir,queryDB:get_id(filename:dirname(RealDst))),
                    [SrcFileIDList, SrcDirIDList] = queryDB:get_all_sub_files(SrcFileID),
                    queryDB:add_moved_files(SrcDirIDList,FullSrc,RealDst),
                    queryDB:add_moved_files(SrcFileIDList,FullSrc,RealDst),
                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                caseFileToUnFile when CopyFileAcl->
                    SrcFileID = queryDB:get_id(FullSrc),
                    {CT,MT} = queryDB:get_time(SrcFileID),
                    queryDB:add_one_row(SrcFileID,FullDst,CT,MT,file,queryDB:get_id(filename:dirname(FullDst))),
                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                caseFileToDir when CopyFileAcl->
                    SrcFileID = queryDB:get_id(FullSrc),
                    queryDB:add_moved_files([SrcFileID],filename:dirname(FullSrc),RealDst),
                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                _Any ->
                    {error, "sorry, you are not authorized to do this operation "}
            end;
        {error, _} ->
            {error, "wrong input"}
    end
.
do_list(FilePathName,UserName)->
    RealFilePathName = get_real_path(FilePathName),
    case gen_server:call(?ACL_SERVER, {read, filename:dirname(RealFilePathName), UserName}) and (queryDB:get_tag(RealFilePathName)=:=dir) of
        true ->
            ParentDirID = queryDB:get_id(RealFilePathName),
            queryDB:get_direct_sub_files(ParentDirID);
        false ->
            {error, "not a dir or you are not authorized to do this operation "}
    end 
.
do_mkdir(PathName, UserName)->
    RealPathName = get_real_path(PathName),
    case queryDB:get_tag(RealPathName) of
        null ->
            ParentDirName = filename:dirname(RealPathName),
            case gen_server:call(?ACL_SERVER, {write, ParentDirName, UserName})  and (queryDB:get_tag(ParentDirName)=:=dir) of
                true ->
                    queryDB:add_new_dir(lib_uuid:uuid_gen(),RealPathName,queryDB:get_id(ParentDirName));
                false ->
                    {error, "sorry, you are not authorized to do this operation "}
            end;
        _Any ->
            {error, "file with this name exists already"}
    end
.
do_chmod(FileName, UserName, UserType, CtrlACL) ->
    case queryDB:get_tag(FileName) of
        null ->
            {error, "file not exists"};
        _Any ->
            gen_server:call(?ACL_SERVER, {setacl, FileName, UserName, UserType, CtrlACL})
    end
.
do_setroot(RootDir) ->
    ?ROOT_DIR = RootDir,
    {ok, ?ROOT_DIR}
.

check_op_type(SrcFullPath, DstFullPath) ->
    SrcUnderDst = filename:join(DstFullPath,filename:basename(SrcFullPath)),
    DstParent = filename:dirname(DstFullPath),

    SrcTag = queryDB:get_tag(SrcFullPath),
    DstTag = queryDB:get_tag(DstFullPath),
    SrcUnderDstTag = queryDB:get_tag(SrcUnderDst),
    DstParentTag = queryDB:get_tag(DstParent),

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

get_real_path(FilePathName) ->
    case string:equal(?ROOT_DIR, "/") of
        true ->
            lists:append(?ROOT_DIR, FilePathName);
        false ->
            lists:append(lists:append(?ROOT_DIR, "/"), FilePathName)
    end
.

call_meta_open(_FileID, _Mode)->
    true
.
call_meta_new() ->
    true
.
call_meta_delete(_FileID)->
    true
.
call_meta_delete(list, _FileIDList) ->
    true
.
call_meta_copy(_FileID) ->
    true
.
call_meta_copy(list, _SrcFileIDList, _DstFileIDList) ->
    true
.
call_meta_check(_FileID)->
    true
.
call_meta_check(list,_SrcFileIDList) ->
    true
.
uuid_gen_N(0) ->
    [];
uuid_gen_N(N) ->
    lists:append([lib_uuid:uuid_gen()],uuid_gen_N(N-1))
.

