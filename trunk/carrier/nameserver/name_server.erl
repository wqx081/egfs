-module(name_server).
%-export([start/0,stop/0,terminate/2]).
%-export([init/1, handle_call/3, handle_cast/2,handle_info/2]).
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
%% UserName -> <<integer():64>>
%% return -> {ok, FileID} | {error, []}
handle_call({open, FilePathName, Mode, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_open, FileName:~p,Mode:~p,Token:~p~n",[FilePathName,Mode,UserName]),
    Reply = do_open(FilePathName, Mode, UserName),
    {reply, Reply, State};
%%"name server" methods
%% 2: delete file
%% FilePathName->string().
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({delete, FilePathName, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_delete, FilePathName:~p,UserName:~p~n",[FilePathName,UserName]),
    Reply = do_delete(FilePathName, UserName),
    {reply, Reply, State};
%%"name server" methods
%% 3: copy file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({copy, SrcFilePathName, DstFilePathName, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_copy, SrcFilePathName:~p, DstFilePathName:~p, UserName:~p~n",[SrcFilePathName, DstFilePathName, UserName]),
    Reply = do_copy(SrcFilePathName, DstFilePathName, UserName),
    {reply, Reply, State};
%%"name server" methods
%% 4: move file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({move, SrcFilePathName, DstFilePathName, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_move, SrcFilePathName:~p, DstFilePathName:~p, UserName:~p~n",[SrcFilePathName, DstFilePathName, UserName]),
    Reply = do_move(SrcFilePathName, DstFilePathName, UserName),
    {reply, Reply, State};
%%"name server" methods
%% 5: list file/directory
%% FilePathName->string().
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({list, FilePathName, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_list, FilePathName:~p,UserName:~p~n",[FilePathName, UserName]),
    Reply = do_list(FilePathName, UserName),
    {reply, Reply, State};
%%"name server" methods
%% 6: mkdir file/directory
%% PathName->string().
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({mkdir, PathName, UserName}, {_From, _}, State) ->
    io:format("inside handle_call_mkdir, PathName:~p,UserName:~p~n",[PathName, UserName]),
    Reply = do_mkdir(PathName, UserName),
    {reply, Reply, State};
%%"name server" methods
%% 7: change mod
%% FileName->string().
%% UserName->string().
%% UserType->user/group
%%CtrlACL->0~7
%% UserName -> <<integer():64>>
%% return -> {ok, []} | {error, []}
handle_call({chmod, FileName, UserName, UserType, CtrlACL}, {_From, _}, State) ->
    io:format("inside handle_call_chmod, FileName:~p~n UserName:~p~n UserType:~p~n CtrlACL:~p~n",[FileName, UserName, UserType, CtrlACL]),
    Reply = do_chmod(FileName, UserName, UserType, CtrlACL),
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

open(FilePathName, Mode, UserName) ->
    gen_server:call(?NAME_SERVER, {open, FilePathName, Mode, UserName}).
delete(FilePathName, UserName)->
    gen_server:call(?NAME_SERVER, {delete, FilePathName, UserName}).
copy(SrcFilePathName, DstFilePathName, UserName)->
    gen_server:call(?NAME_SERVER, {copy, SrcFilePathName, DstFilePathName, UserName}).
move(SrcFilePathName, DstFilePathName, UserName)->
    gen_server:call(?NAME_SERVER, {move, SrcFilePathName, DstFilePathName, UserName}).
list(FilePathName, UserName)->
    gen_server:call(?NAME_SERVER, {list, FilePathName, UserName}).
mkdir(PathName, UserName)->
    gen_server:call(?NAME_SERVER, {mkdir, PathName, UserName}).
chmod(FileName, UserName, UserType, CtrlACL)->
    gen_server:call(?NAME_SERVER, {chmod, FileName, UserName, UserType, CtrlACL}).

do_open(FilePathName, Mode, UserName) ->
    %% gen_server:call(?ACL_SERVER, {Mode, FilePathName, UserName})
    case get_acl() of
        true ->
            case queryDB:get_tag(FilePathName) of
                file ->
                    FileID = queryDB:get_id(FilePathName),
                    call_meta_open(FileID, Mode);
                dir ->
                    {error, "you are opening a dir"};
                _Any ->
                    ParentDir = filename:dirname(FilePathName),
                    %%gen_server:call(?ACL_SERVER, {write, ParentDir, UserName})
                    case ((Mode=:=write) and queryDB:get_tag(ParentDir)=:=dir) and get_acl() of
                        true ->
                            FileID = lib_uuid:gen(),
                            case call_meta_open(FileID, write) of
                                true ->
                                    queryDB:add_new_file(FileID,FilePathName,queryDB:get_id(ParentDir));
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
    %%gen_server:call(?ACL_SERVER, {delete_folder, FilePathName, UserName})
    DeleteDir = ((queryDB:get_tag(FilePathName)=:=dir) and get_acl()),
    %%gen_server:call(?ACL_SERVER, {delete_folder, FilePathName, UserName})
    DeleteFile = ((queryDB:get_tag(FilePathName)=:=file) and get_acl()),
    if
        DeleteDir or DeleteFile->
            FileID = queryDB:get_id(FilePathName),
            [FileIDList, DirIDList] = queryDB:get_all_sub_files(FileID),
            AllDirIDList = lists:append(DirIDList,[FileID]),
            case call_meta_delete(list, FileIDList) of
                true ->
                    queryDB:delete_rows(FileIDList),
                    queryDB:delete_rows(AllDirIDList),
                    %%gen_server:call(?ACL_SERVER, {delete_aclrecord, FilePathName});
                    get_acl();
                false ->
                    {error, "sorry, someone is using this file, unable to delete it now"}
            end;
        true ->
            {error, "file does not exist or you are not authorized to do this operation"}
    end
.
do_copy(FullSrc, FullDst, UserName)->
    CheckResult = check_op_type(FullSrc, FullDst),    
    case CheckResult of
        {ok, _,_,_} ->
            %% 冗余，待整理
            {ok,Case,DirToAcl,RealDst} = CheckResult,            
            %%gen_server:call(?ACL_SERVER, {copyfolder, FullSrc, DirToAcl,UserName})
            CopyDir = ((Case=:=caseDirToDir) or (Case=:=caseDirToUnDir))
            and get_acl(),
            %%gen_server:call(?ACL_SERVER, {copyfolder, FullSrc, DirToAcl,UserName})
            CopyFile = ((Case=:=caseFileToDir) or (Case=:=caseFileToUnFile))
            and get_acl(),
            if
                CopyDir ->
                    queryDB:add_new_dir(lib_uuid:gen(),RealDst,queryDB:get_id(filename:dirname(RealDst))),
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
                    DstFileID = lib_uuid:gen(),
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
do_move(FullSrc, FullDst, UserName)->
    CheckResult = check_op_type(FullSrc, FullDst),
    case CheckResult of
        {ok, _,_,_} ->
            %% 冗余，待整理
            {ok,Case,DirToAcl,RealDst} = CheckResult,
            %%gen_server:call(?ACL_SERVER, {copyfoder, FullSrc, DirToAcl, UserName}) and gen_server:call(?ACL_SERVER, {delete, FullSrc, UserName})
            CopyDirAcl = get_acl(),
            %%gen_server:call(?ACL_SERVER, {copyfile, FullSrc, DirToAcl, UserName}) and gen_server:call(?ACL_SERVER, {delete, FullSrc, UserName})
            CopyFileAcl = get_acl(),
            case Case of
                caseDirToUnDir when CopyDirAcl->
                    SrcFileID = queryDB:get_id(FullSrc),
                    {CT,MT} = queryDB:get_time(SrcFileID),
                    queryDB:add_one_row(SrcFileID,RealDst,CT,MT,dir,queryDB:get_id(filename:dirname(RealDst))),
                    [SrcFileIDList, SrcDirIDList] = queryDB:get_all_sub_files(SrcFileID),
                    queryDB:add_moved_files(SrcDirIDList,FullSrc,RealDst),
                    queryDB:add_moved_files(SrcFileIDList,FullSrc,RealDst),
                    %                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                    get_acl();
                caseDirToDir when CopyDirAcl->
                    SrcFileID = queryDB:get_id(FullSrc),
                    {CT,MT} = queryDB:get_time(SrcFileID),
                    queryDB:add_one_row(SrcFileID,RealDst,CT,MT,dir,queryDB:get_id(filename:dirname(RealDst))),
                    [SrcFileIDList, SrcDirIDList] = queryDB:get_all_sub_files(SrcFileID),
                    queryDB:add_moved_files(SrcDirIDList,FullSrc,RealDst),
                    queryDB:add_moved_files(SrcFileIDList,FullSrc,RealDst),
                    %                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                    get_acl();
                caseFileToUnFile when CopyFileAcl->
                    SrcFileID = queryDB:get_id(FullSrc),
                    {CT,MT} = queryDB:get_time(SrcFileID),
                    queryDB:add_one_row(SrcFileID,FullDst,CT,MT,file,queryDB:get_id(filename:dirname(FullDst))),
                    %                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                    get_acl();
                caseFileToDir when CopyFileAcl->
                    SrcFileID = queryDB:get_id(FullSrc),
                    queryDB:add_moved_files([SrcFileID],filename:dirname(FullSrc),RealDst),
                    %                    gen_server:call(?ACL_SERVER, {delete_aclrecord, FullSrc});
                    get_acl();
                _Any ->
                    {error, "sorry, you are not authorized to do this operation "}
            end;
        {error, _} ->
            {error, "wrong input"}
    end
.
do_list(FilePathName,UserName)->
    %    gen_server:call(?ACL_SERVER, {read, filename:dirname(FilePathName), UserName})
    case get_acl() and (queryDB:get_tag(FilePathName)=:=dir) of
        true ->
            ParentDirID = queryDB:get_id(FilePathName),
            queryDB:get_direct_sub_files(ParentDirID);
        false ->
            {error, "not a dir or you are not authorized to do this operation "}
    end 
.
do_mkdir(PathName, UserName)->
    case queryDB:get_tag(PathName) of
        null ->
            ParentDirName = filename:dirname(PathName),
            %%gen_server:call(?ACL_SERVER, {write, ParentDirName, UserName})
            case get_acl()  and (queryDB:get_tag(ParentDirName)=:=dir) of
                true ->
                    queryDB:add_new_dir(lib_uuid:gen(),PathName,queryDB:get_id(ParentDirName));
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
            %            gen_server:call(?ACL_SERVER, {setacl, FileName, UserName, UserType, CtrlACL})
            get_acl()
    end
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
    lists:append([lib_uuid:gen()],uuid_gen_N(N-1))
.
get_acl() ->
    true
.
