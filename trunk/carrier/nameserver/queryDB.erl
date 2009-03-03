-module(queryDB).
-include_lib("stdlib/include/qlc.hrl").

-import(lists, [foreach/2]).
-compile(export_all).

%-export([do_this_once/0, start_start_mnesia/0]).
%-record(dirmeta, {filename, fileid, filesize, createT, modifyT, acl}).
%-record(dirmetaTable, {dirname, somearg}).
%-record(clientinfo, {clientid, modes}).
%-record(filesessionTable, {fileid, clientlist}).
-record(dirmeta, {id = <<0:64>>, filename, createT, modifyT, tag, parent}).

%% start of mnesia
do_this_once() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(dirmeta, [{attributes, record_info(fields, dirmeta)}]),
    mnesia:stop().

start_mnesia()->
    mnesia:start(),
    mnesia:wait_for_tables([dirmeta], 20000).

% test info
example_tables() ->
    [
        %%-record(dirmeta, {fileid = <<0:64>>, filename, createT, modifyT, tag, parent}).

     {dirmeta,  101, "/A/B/f1", any, any, file, 105},
         {dirmeta,  102, "/A/B/f2", any, any, file, 105},
         {dirmeta,  103, "/A/B/f3", any, any, file, 105},

         {dirmeta,  107, "/A/B/C/f4", any, any, file, 104},

         {dirmeta,  104, "/A/B/C", any, any, dir, 105},
         {dirmeta,  105, "/A/B", any, any, dir, 106},

         {dirmeta,  108, "/A/D/C", any, any, dir, 109},
         {dirmeta,  109, "/A/D", any, any, dir, 106},
         {dirmeta, 110, "/A/E", any, any, dir, 106},

         {dirmeta,  106, "/A", any, any, dir, any}
     %         {dirmetaTable, "A/B/C4"},
     %         {dirmetaTable, "A/B/C5"},
     %         {dirmetaTable, "A/B/C6"},
     %         {dirmetaTable, "A/B/C7"},
     %         {dirmetaTable, "A/B/C8"},
     %         {dirmetaTable, "A/B/C9"}
    ].

reset_tables() ->
    mnesia:clear_table(dirmeta),
    F = fun() ->
                foreach(fun mnesia:write/1, example_tables())
        end,
    mnesia:transaction(F).

do_this() ->
	do_this_once(),
	start_mnesia(),
	reset_tables()
.

do(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Val} = mnesia:transaction(F),
    Val
.
%% end of mnesia



get_tag(Dir) ->
    Result = do(qlc:q([X#dirmeta.tag||X<-mnesia:table(dirmeta), X#dirmeta.filename=:=Dir])),
    case Result of
        [] ->
            null;
        [Tag] ->
            Tag
    end
.
get_tag_by_id(ID) ->
    Result = do(qlc:q([X#dirmeta.tag||X<-mnesia:table(dirmeta), X#dirmeta.id=:=ID])),
    case Result of
        [] ->
            null;
        [Tag] ->
            Tag
    end
.
get_name(ID) ->
    Result = do(qlc:q([X#dirmeta.filename||X<-mnesia:table(dirmeta), X#dirmeta.id=:=ID])),
    case Result of
        [] ->
            null;
        [Name|_T] ->
            Name
    end
.
get_id(Dir) ->
    Result = do(qlc:q([X#dirmeta.id||X<-mnesia:table(dirmeta), X#dirmeta.filename=:=Dir])),
    case Result of
        [] ->
            null;
        [ID|_T] ->
            ID
    end
.
get_time(ID) ->
    Result = do(qlc:q([{X#dirmeta.createT,X#dirmeta.modifyT}||X<-mnesia:table(dirmeta), X#dirmeta.id=:=ID])),
    case Result of
        [] ->
            null;
        [{CT,MT}|_T] ->
            {CT,MT}
    end
.
delete_rows([]) ->
    ok;
delete_rows(IDList) ->
    [Head|Left] = IDList,
    delete_one_row(Head),
    delete_rows(Left)
.
delete_one_row(ID) ->
    Row = {dirmeta, ID},
    F = fun() ->
                mnesia:delete(Row)
        end,
    mnesia:transaction(F)
.
add_one_row(ID,FileName,CreateTime,ModifyTime,Tag,ParentID) ->
    Row = #dirmeta{id=ID,filename=FileName,tag=Tag,createT=CreateTime,modifyT=ModifyTime,parent=ParentID},
    F = fun() ->
                mnesia:write(Row)
        end,
    mnesia:transaction(F)
.
get_all_sub_files(FileID) ->
    case queryDB:get_tag_by_id(FileID) of
        file ->
            [[FileID],[]];
        dir ->
            [DirectSubFile,DirectSubDir] = get_direct_sub_files(FileID),
            [DirectSubDir_File, DirectSubDir_Dir] = get_all_sub_files(list, DirectSubDir),
            [lists:append(DirectSubFile, DirectSubDir_File),lists:append(DirectSubDir, DirectSubDir_Dir)]
    end
.
get_all_sub_files(list, []) ->
    [[],[]];
get_all_sub_files(list, FileIDList) ->
    [Head|Left] = FileIDList,
    [HeadSubFile,HeadSubDir] = get_all_sub_files(Head),
    [LeftSubFile,LeftSubDir] = get_all_sub_files(list, Left),
    [lists:append(HeadSubFile, LeftSubFile),lists:append(HeadSubDir, LeftSubDir)]
.

get_direct_sub_files(FileID) ->
    Result = do(qlc:q([{X#dirmeta.tag,X#dirmeta.id}||X<-mnesia:table(dirmeta), X#dirmeta.parent=:=FileID])),
    seperate_file_dir(Result)
.
seperate_file_dir([]) ->
    [[],[]];
seperate_file_dir(FileList) ->
    [{Tag, ID}|Left] = FileList,
    [LeftFiles,LeftDirs] = seperate_file_dir(Left),
    case Tag of
        file ->
            [lists:append([ID],LeftFiles),LeftDirs];
        dir ->
            [LeftFiles,lists:append([ID],LeftDirs)]
    end
.
get_modifyed_path(ID, SrcHead, DstHead) ->
    SrcPath = get_name(ID),
    Path = string:substr(SrcPath,string:len(SrcHead)+1,string:len(SrcPath)),
    string:concat(DstHead,Path)
.
add_moved_files([],_FullSrc,_RealDst) ->
    ok;
add_moved_files(SrcFileIDList,FullSrc,RealDst) ->
    [Head|Left] = SrcFileIDList,
    NewPath = get_modifyed_path(Head,FullSrc,RealDst),
    {CT,MT} = queryDB:get_time(Head),
    queryDB:add_one_row(Head,NewPath,CT,MT,file,queryDB:get_id(filename:dirname(NewPath))),
    add_moved_files(Left,FullSrc,RealDst)
.
add_copyed_files([],[],_FullSrc,_RealDst) ->
    [];
add_copyed_files(SrcFileIDList,DstFileIDList,Src,AimDst) ->
    [SrcHead|SrcLeft] = SrcFileIDList,
    [DstHead|DstLeft] = DstFileIDList,
    NewPath = get_modifyed_path(SrcHead,Src,AimDst),
    [{NewCT,NewMT,NewTag}|_T] = do(qlc:q([{X#dirmeta.createT,X#dirmeta.modifyT,X#dirmeta.tag}
        ||X<-mnesia:table(dirmeta),X#dirmeta.id=:=SrcHead])),
    NewParent = get_id(filename:dirname(NewPath)),
    Row = #dirmeta{id=DstHead,filename=NewPath,createT=NewCT,modifyT=NewMT,tag=NewTag,parent=NewParent},
    F = fun() ->
                mnesia:write(Row)
        end,
    mnesia:transaction(F),
    [NewPath|add_copyed_files(SrcLeft,DstLeft,Src,AimDst)]
.
add_new_dir(ID,DirName,ParentID) ->
    Row = #dirmeta{id=ID,filename=DirName,tag=dir,parent=ParentID},
    F = fun() ->
                mnesia:write(Row)
        end,
    mnesia:transaction(F)
.
add_new_file(ID,FileName,ParentID) ->
    Row = #dirmeta{id=ID,filename=FileName,tag=file,createT=term_to_binary(erlang:localtime()),modifyT=term_to_binary(erlang:localtime()),parent=ParentID},
    F = fun() ->
                mnesia:write(Row)
        end,
    mnesia:transaction(F)
.