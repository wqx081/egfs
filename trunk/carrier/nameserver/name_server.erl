-module(name_server).
-include("name_server.hrl").
%-include("../egfs.hrl").

-define(ROOT_DIR, "/home/mere/myroot").

-compile(export_all).

%%"name server" methods
%% 1: open file
%% FilePathName->string().
%% Mode -> w|r|a
%% UserToken -> <<integer():64>>
%% return -> {ok, FileID} | {error, []}
do_open(FilePathName, Mode, _UserToken) ->
    RealFilePathName = get_real_path(FilePathName),
    Return = get_meta_by_path(RealFilePathName),
    case Return of
        {ok, FileMetaRec} ->
            CallMetaOpenResult = call_meta_open(FileMetaRec#fileMetaRec.fileID, Mode),
            CallMetaOpenResult;
        {error, _} ->
            if
                Mode=:=r ->
                    {error, "file does not exist or is a directory"};
                (Mode=:=w) or (Mode=:=a) ->
                    case filelib:is_dir(RealFilePathName) of
                        true ->
                            {error, "filename already exists as a directory"};
                        false -> 
                            CallMetaNewResult= call_meta_new(),
                            case CallMetaNewResult of
                                {ok, FileID} ->
                                    Meta=#fileMetaRec{
                                        fileID = FileID,
                                        fileSize = <<1:64>>,
                                        createTime = term_to_binary(erlang:localtime()),
                                        modifyTime= term_to_binary(erlang:localtime())},
                                    RealPath = get_real_path(FilePathName),
                                    write_meta(RealPath,Meta),
                                    call_meta_open(FileID, Mode);
                                {error, _} ->
                                    CallMetaNewResult
                            end
                    end;
                true ->
                    {error, "unknown open mode"}
            end
    end
.
%%"name server" methods
%% 2: delete file/directory
%% FilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
do_delete(FilePathName, _UserToken)->
    RealFilePath=get_real_path(FilePathName),
    case filelib:is_file(RealFilePath) of
        true ->            
            recursive_do_files(RealFilePath, "", "^[^\\/:*?""<>|,]+$", true, delete);
        _Any ->
            {error, "file does not exist"}
    end
.
%%"name server" methods
%% 3: copy file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
do_copy(SrcFilePathName, DstFilePathName, _UserToken)->
    SrcRealFilePathName = get_real_path(SrcFilePathName),
    DstRealFilePathName = get_real_path(DstFilePathName),
    AimDstRealFilePathName = filename:join(DstRealFilePathName,filename:basename(SrcFilePathName)),
    CaseDirToDir = filelib:is_dir(SrcRealFilePathName) and filelib:is_dir(DstRealFilePathName)  and (not(filelib:is_file(AimDstRealFilePathName))) and (string:rstr(DstRealFilePathName,SrcRealFilePathName)=:=0),
    CaseRegToDir = filelib:is_regular(SrcRealFilePathName) and filelib:is_dir(DstRealFilePathName) and (not(filelib:is_file(AimDstRealFilePathName))) and (string:rstr(DstRealFilePathName,SrcRealFilePathName)=:=0),
    CaseDirToUnDir = filelib:is_dir(SrcRealFilePathName) and not(filelib:is_file(DstRealFilePathName)) and (filelib:is_dir(filename:dirname(DstRealFilePathName))),
    CaseRegToUnReg = filelib:is_regular(SrcRealFilePathName) and (not(filelib:is_file(DstRealFilePathName))) and (filelib:is_dir(filename:dirname(DstRealFilePathName))),
    %    io:format("dir-dir:~p~nreg-dir:~p~ndir-UNdir:~p~nreg-UNreg:~p~n",[CaseDirToDir,CaseRegToDir,CaseDirToUnDir,CaseRegToUnReg]),
    if
        CaseDirToDir ->
            recursive_do_files(SrcRealFilePathName, AimDstRealFilePathName, "^[^\\/:*?""<>|,]+$", true, copy);
        CaseDirToUnDir ->
            recursive_do_files(SrcRealFilePathName, DstRealFilePathName, "^[^\\/:*?""<>|,]+$", true, copy);
        CaseRegToDir ->
            recursive_do_files(SrcRealFilePathName, DstRealFilePathName, "^[^\\/:*?""<>|,]+$", true, copy);
        CaseRegToUnReg ->
            recursive_do_files(SrcRealFilePathName, DstRealFilePathName, "^[^\\/:*?""<>|,]+$", true, copy);
        true ->
            {error, "source/destination is wrong or destination file already exists."}
    end
.
%%"name server" methods
%% 4: move file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
do_move(SrcFilePathName, DstFilePathName, _UserToken)->
    SrcRealFilePathName = get_real_path(SrcFilePathName),
    DstRealFilePathName = get_real_path(DstFilePathName),
    AimDstRealFilePathName = filename:join(DstRealFilePathName,filename:basename(SrcFilePathName)),
    CaseDirToDir = filelib:is_dir(SrcRealFilePathName) and filelib:is_dir(DstRealFilePathName)  and (not(filelib:is_file(AimDstRealFilePathName))) and (string:rstr(DstRealFilePathName,SrcRealFilePathName)=:=0),
    CaseRegToDir = filelib:is_regular(SrcRealFilePathName) and filelib:is_dir(DstRealFilePathName) and (not(filelib:is_file(AimDstRealFilePathName))) and (string:rstr(DstRealFilePathName,SrcRealFilePathName)=:=0),
    CaseDirToUnDir = filelib:is_dir(SrcRealFilePathName) and not(filelib:is_file(DstRealFilePathName)) and (filelib:is_dir(filename:dirname(DstRealFilePathName))),
    CaseRegToUnReg = filelib:is_regular(SrcRealFilePathName) and (not(filelib:is_file(DstRealFilePathName))) and (filelib:is_dir(filename:dirname(DstRealFilePathName))),
    %    io:format("dir-dir:~p~nreg-dir:~p~ndir-UNdir:~p~nreg-UNreg:~p~n",[CaseDirToDir,CaseRegToDir,CaseDirToUnDir,CaseRegToUnReg]),
    if
        CaseDirToDir ->
            recursive_do_files(SrcRealFilePathName, AimDstRealFilePathName, "^[^\\/:*?""<>|,]+$", true, move);
        CaseDirToUnDir ->
            recursive_do_files(SrcRealFilePathName, DstRealFilePathName, "^[^\\/:*?""<>|,]+$", true, move);
        CaseRegToDir ->
            recursive_do_files(SrcRealFilePathName, DstRealFilePathName, "^[^\\/:*?""<>|,]+$", true, move);
        CaseRegToUnReg ->
            recursive_do_files(SrcRealFilePathName, DstRealFilePathName, "^[^\\/:*?""<>|,]+$", true, move);
        true ->
            {error, "source/destination is wrong or destination file already exists."}
    end
.
%%"name server" methods
%% 5: list file/directory
%% FilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
do_list(FilePathName, _UserToken)->
    RealFilePathName = get_real_path(FilePathName),
    case file:list_dir(RealFilePathName) of
        {ok, Files} ->
            {ok, get_file_info(Files,RealFilePathName)};
        {error, _} ->
            {error, "fail to list this directory"}
    end
.
get_file_info([],_RealFilePathName) -> [];
get_file_info([File|T],RealFilePathName) ->
    RealFullName = filename:join(RealFilePathName, File),
    case filelib:is_regular(RealFullName) of
        true ->            
            GetMetaResult = get_meta_by_path(RealFullName),
            case GetMetaResult of
                %if the file is not a meta file, then ignore it% do we need that? or we can ensure no exceptional file exists
                {ok, FileMetaRec} ->
                    [{regular, File,
                      FileMetaRec#fileMetaRec.fileSize,
                      FileMetaRec#fileMetaRec.createTime,
                      FileMetaRec#fileMetaRec.modifyTime}|get_file_info([X||X<-T],RealFilePathName)];
                {error, _} ->
                    [get_file_info([X||X<-T],RealFilePathName)]
            end;
        false ->
            [{dir, File, <<0:64>>, <<>>, <<>>}|get_file_info([X||X<-T],RealFilePathName)]
    end
.
%%"name server" methods
%% 6: mkdir directory : only one_level_down sub dirctory
%% PathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
do_mkdir(PathName, _UserToken)->
    RealPathName = get_real_path(PathName),
    case filelib:is_file(RealPathName) of
        true ->
            {error, "file with this name exists already"};
        false ->
            file:make_dir(RealPathName)
    end
.

get_meta_by_path(RealFilePathName)->
    case filelib:is_regular(RealFilePathName) =:= true of
        true ->
            read_meta(RealFilePathName);
        false ->
            {error, "file does not exists or file to read is not a regular file"}
    end
.
get_real_path("") -> ?ROOT_DIR;
get_real_path(FilePathName) ->
    [UserPathHead | _Any] = FilePathName,
    case UserPathHead =:= 47 of % 47 = ASCII("/")
        true ->
            UserPath1 = FilePathName;
        false ->
            UserPath1 = lists:append("/", FilePathName)
    end,
    lists:append(?ROOT_DIR, UserPath1)
.

call_meta_open(FileID, _Mode)->
    {ok, FileID}
.
call_meta_new() ->
    {ok, <<2:64>>}
.
call_meta_delete(_FileID)->
    {ok, []}
.
call_meta_copy(_FileID) ->
    {ok, <<1:64>>}
.
call_meta_check(FileID)->
    {ok, FileID}
.
read_meta(Path) ->    
	case file:read_file(Path) of
		{ok,<<Binary/binary>>} ->
			Meta = binary_to_term(Binary),
			{ok,Meta};
		{error,enoent} ->
			{error,enoent};
		{error,Reason} ->
			{error,Reason}
	end
.
write_meta(Path,Meta) ->
	Data = term_to_binary(Meta),
	file:write_file(Path,Data,[write,raw,binary])
.

%% SrcDir & DstDir are real pathes
recursive_do_files(SrcDir, DstDir, RegExp, Recursive, Mode) ->
    {ok, Re1} = regexp:parse(RegExp),
    DoResult = recursive_do_files1(SrcDir, DstDir, Re1, Recursive, Mode),
    case DoResult of
        {ok, _}  ->
            {ok, []};
        ok ->
            {ok, []};
        true ->
            {error, []}
    end
.
recursive_do_files1(SrcDir, DstDir, RegExp, Recursive, Mode) ->
    case filelib:is_regular(SrcDir) of
        true  ->
            recursive_do_files2([filename:basename(SrcDir)], filename:dirname(SrcDir), DstDir, RegExp, Recursive, Mode);
        false ->
            case file:list_dir(SrcDir) of
                {ok, Files} ->
                    case Mode of
                        delete ->
                            recursive_do_files2(Files, SrcDir, DstDir, RegExp, Recursive, Mode),
                            file:del_dir(SrcDir);
                        copy -> 
                            %                            AimDstDir = filename:join(DstDir, filename:basename(SrcDir)),
                            file:make_dir(DstDir),
                            recursive_do_files2(Files, SrcDir, DstDir, RegExp, Recursive, Mode);
                        move ->
                            %                            AimDstDir = filename:join(DstDir, filename:basename(SrcDir)),
                            file:make_dir(DstDir),
                            recursive_do_files2(Files, SrcDir, DstDir, RegExp, Recursive, Mode),
                            file:del_dir(SrcDir)
                    end;
                {error, _}  ->
                    {error, "error in listing a dir"}
            end
    end
.
recursive_do_files2([], _SrcDir, _DstDir, _RegExp, _Recursive, _Mode) ->
    {ok, []};
recursive_do_files2([File|T], SrcDir, DstDir, RegExp, Recursive, Mode) ->
    SrcFullName = filename:join(SrcDir, File),
    case filelib:is_dir(DstDir) of
        true ->
            DstFullName = filename:join(DstDir, File);
        false ->
            DstFullName = DstDir
    end,
    case filelib:is_regular(SrcFullName) of
        true  ->
            case regexp:match(File, RegExp) of
                {match, _, _}  ->
                    case Mode of
                        delete ->
                            %SrcFullName is ensured as a regular, so no need to deal with error
                            {ok, FileMetaRec} = get_meta_by_path(SrcFullName),
                            CallMetaDeleteResult = call_meta_delete(FileMetaRec#fileMetaRec.fileID),
                            case CallMetaDeleteResult of
                                {ok, _} ->
                                    file:delete(SrcFullName);
                                {error, _} ->
                                    CallMetaDeleteResult
                            end,
                            recursive_do_files2(T, SrcDir, DstDir, RegExp, Recursive, Mode);
                        copy ->
                            {ok, FileMetaRec} = get_meta_by_path(SrcFullName),
                            % metaserver give me a new FileID
                            CallMetaCopyResult = call_meta_copy(FileMetaRec#fileMetaRec.fileID),
                            case CallMetaCopyResult of
                                {ok, FileID} ->
                                    Meta=#fileMetaRec{
                                        fileID = FileID,
                                        fileSize = FileMetaRec#fileMetaRec.fileSize,
                                        createTime = term_to_binary(erlang:localtime()),
                                        modifyTime= term_to_binary(erlang:localtime())},
                                    write_meta(DstFullName,Meta);
                                %file:copy(SrcFullName, DstFullName)
                                {error, _} ->
                                    CallMetaCopyResult
                            end,
                            recursive_do_files2(T, SrcDir, DstDir, RegExp, Recursive, Mode);
                        move ->
                            {ok, FileMetaRec} = get_meta_by_path(SrcFullName),
                            CallMetaCheckResult = call_meta_check(FileMetaRec#fileMetaRec.fileID),
                            case CallMetaCheckResult of
                                {ok, _} ->
                                    file:copy(SrcFullName, DstFullName),
                                    file:delete(SrcFullName);
                                {error, _} ->
                                    CallMetaCheckResult
                            end,
                            recursive_do_files2(T, SrcDir, DstDir, RegExp, Recursive, Mode)
                    end;
                _ ->
                    recursive_do_files2(T, SrcDir, DstDir, RegExp, Recursive, Mode)
            end;
        false ->
            case Recursive and filelib:is_dir(SrcFullName) of
                true ->
                    recursive_do_files1(SrcFullName, DstFullName, RegExp, Recursive, Mode),
                    recursive_do_files2(T, SrcDir, DstDir, RegExp, Recursive, Mode);
                false ->
                    recursive_do_files2(T, SrcDir, DstDir, RegExp, Recursive, Mode)
            end
    end
.