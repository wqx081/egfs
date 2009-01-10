-module(name_server).
-include("name_server.hrl").
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
            call_meta_open(FileMetaRec#fileMetaRec.fileID, Mode);
        {error, _} ->
            case Mode of
                r ->
                    {error, "file does not exist"};
                w ->
                    {ok, FileID}= call_meta_new(),
                    % TODO: change the info of new meta file
                    Meta=#fileMetaRec{
                        fileID = FileID,
                        fileSize = <<1:64>>,
                        createTime = "20080101", modifyTime= "20090101"},
                    RealPath = get_real_path(FilePathName),
                    write_meta(RealPath,Meta),
                    call_meta_open(FileID, Mode);
                _ ->
                    {error, "unknown open mode"}
            end
    end
.
%%"name server" methods
%% 2: delete file/directory
%% FilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
do_delete(FilePathName, UserToken)->
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
do_copy(SrcFilePathName, DstFilePathName, UserToken)->
    % source file is legal & destination file does not exist
    SrcRealFilePathName = get_real_path(SrcFilePathName),
    DstRealFilePathName = get_real_path(DstFilePathName),
    case filelib:is_file(SrcRealFilePathName)  and not(filelib:is_file(DstRealFilePathName)) of
        true ->
            recursive_do_files(SrcRealFilePathName, DstRealFilePathName, "^[^\\/:*?""<>|,]+$", true, copy);
        false ->
            {error, "source file does not exist or destination file already exists"}
    end
.
%%"name server" methods
%% 4: move file/directory
%% SrcFilePathName->string().
%% DstFilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
do_move(SrcFilePathName, DstFilePathName, UserToken)->
    SrcRealFilePathName = get_real_path(SrcFilePathName),
    DstRealFilePathName = get_real_path(DstFilePathName),
    case filelib:is_file(SrcRealFilePathName)  and not(filelib:is_file(DstRealFilePathName)) of
        true ->
            recursive_do_files(SrcRealFilePathName, DstRealFilePathName, "^[^\\/:*?""<>|,]+$", true, move);
        false ->
            {error, "source file does not exist or destination file already exists"}
    end
.
%%"name server" methods
%% 5: list file/directory
%% FilePathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
do_list(FilePathName, UserToken)->
    RealFilePathName = get_real_path(FilePathName),
    file:list_dir(RealFilePathName)
.
%%"name server" methods
%% 6: mkdir directory : only one_level_down sub dirctory
%% PathName->string().
%% UserToken -> <<integer():64>>
%% return -> {ok, []} | {error, []}
do_mkdir(PathName, UserToken)->
    RealPathName = get_real_path(PathName),
    case filelib:is_file(RealPathName) of
        true ->
            {error, "file exist"};
        false ->
            file:make_dir(RealPathName),
            {ok, []}
    end
.

get_meta_by_path(RealFilePathName)->
    case filelib:is_regular(RealFilePathName) =:= true of
        true ->
            read_meta(RealFilePathName);
        false ->
            {error, "file to read is not a regular file"}
    end
.
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

call_meta_open(FileID, Mode)->
    {ok, FileID}
.
call_meta_new() ->
    {ok, <<2:64>>}
.
call_meta_delete(FileID)->
    {ok, []}
.
call_meta_copy(FileID) ->
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
    recursive_do_files1(SrcDir, DstDir, Re1, Recursive, Mode)
.
recursive_do_files1(SrcDir, DstDir, RegExp, Recursive, Mode) ->
    case file:list_dir(SrcDir) of
        {ok, Files} ->
            case Mode of
                delete ->
                    recursive_do_files2(Files, SrcDir, DstDir, RegExp, Recursive, Mode),
                    file:del_dir(SrcDir);
                copy -> file:make_dir(DstDir),
                    recursive_do_files2(Files, SrcDir, DstDir, RegExp, Recursive, Mode);
                move -> file:make_dir(DstDir),
                    recursive_do_files2(Files, SrcDir, DstDir, RegExp, Recursive, Mode),
                    file:del_dir(SrcDir)
            end;
        {error, _}  -> {SrcDir, DstDir}
    end
.
recursive_do_files2([], SrcDir, DstDir, _RegExp, _Recursive, _Mode) ->
    {ok};
recursive_do_files2([File|T], SrcDir, DstDir, RegExp, Recursive, Mode) ->
    SrcFullName = filename:join(SrcDir, File),
    DstFullName = filename:join(DstDir, File),
    case filelib:is_regular(SrcFullName) of
        true  ->
            case regexp:match(File, RegExp) of
                {match, _, _}  ->
                    case Mode of
                        delete ->
                            {ok, FileMetaRec} = get_meta_by_path(SrcFullName),
                            case call_meta_delete(FileMetaRec#fileMetaRec.fileID) of
                                {ok, _} ->
                                    file:delete(SrcFullName)
                            end,
                            recursive_do_files2(T, SrcDir, DstDir, RegExp, Recursive, Mode);
                        copy ->
                            {ok, FileMetaRec} = get_meta_by_path(SrcFullName),
                            case {ok, FileID} = call_meta_copy(FileMetaRec#fileMetaRec.fileID) of
                                {ok, _} ->
                                    file:copy(SrcFullName, DstFullName)
                            end,
                            recursive_do_files2(T, SrcDir, DstDir, RegExp, Recursive, Mode);
                        move ->
                            {ok, FileMetaRec} = get_meta_by_path(SrcFullName),
                            case {ok, FileID} = call_meta_check(FileMetaRec#fileMetaRec.fileID) of
                                {ok, _} ->
                                    file:copy(SrcFullName, DstFullName),
                                    file:delete(SrcFullName)
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