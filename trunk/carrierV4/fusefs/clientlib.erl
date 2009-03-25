-module(clientlib).
-include("../include/header.hrl").
-include_lib("kernel/include/file.hrl").

-export([ open/2,
	  close/1,
	  delete/1,
	  read_file_info/1,
	  pread/3,
	  pwrite/3,
	  listdir/1,
	  mkdir/1,
	  deldir/1,
	  move/2,
	  copy/2,
	  chmod/4]).

%% -define(CLIENT_SERVER, {client_server, flyclient1@fly}).
-define(CLIENT_SERVER, {client_server, ltclient1@lt}).

min(A, B) ->
    if 
	A > B ->
	    B;
	true  ->
	    A
    end.

rm_slash(Path) ->
    filename:absname(Path).

%%--------------------------------------------------------------------------------
%% Function: open(string(),Mode) -> {ok, ClientWorkerPid} | {error, Reason} 
%% 			 Mode = r | w 
%% Description: open a file according to the filename and open mode
%%--------------------------------------------------------------------------------
open(FileName, Mode) ->
    UserName = any,
    case gen_server:call(?CLIENT_SERVER, {open, FileName, Mode, UserName}) of
	{error, _Reason} ->
	    {error, _Reason};
	{ok, WorkerPid} ->
	    {ok, WorkerPid}
    end.


%% --------------------------------------------------------------------
%% Function: close/1 ->  ok | {error, Reason} 
%% Description: close file
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------
close(WorkerPid) ->
    gen_server:call(WorkerPid, {close}).
	
%% --------------------------------------------------------------------
%% Function: delete/1 ->  ok | {error, Reason} 
%% Description: delete file
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------
delete(FileName)->
    UserName = any,
    CFName = rm_slash(FileName),
    case gen_server:call(?CLIENT_SERVER, {delete, CFName, UserName}) of
	{ok, _} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

%% --------------------------------------------------------------------
%% Function: read_file_info/1 ->  {ok, #file_info{}} | {error, Reason} 
%% Description: read file meta info 
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
read_file_info(FileName) ->
    UserName = any,
    CFName = rm_slash(FileName),
    case gen_server:call(?CLIENT_SERVER, {getfileinfo, CFName, UserName}) of
	[] ->
	    {error, enoent};
	[error, _] ->
	    {error, enoent};
	[H|_] ->
	    {ok, H}
    end.
	    
	
%% --------------------------------------------------------------------
%% Function: listdir/1 ->  {ok, filename_list} | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
listdir(Dir) ->
    UserName = any,
    CDir = rm_slash(Dir),
    case gen_server:call(?CLIENT_SERVER, {list, CDir, UserName}) of
	{error, _R} ->
	    {error, enoent};
	[] ->
	    {ok, []};
	[H|T] ->
	    Namelist = parse_names([H|T], []),
	    {ok, Namelist} 
    end.
	
parse_names([], Names) ->
    lists:reverse(Names);
parse_names([H|T], Names) ->
    {_, _, FullPath} = H,
    Name = filename:basename(FullPath),
    NewNames = [Name | Names],
    parse_names(T, NewNames).

%% --------------------------------------------------------------------
%% Function: mkdir/1 ->  ok | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
mkdir(Dir) ->
    UserName = any,
    CDir = rm_slash(Dir),
    case gen_server:call(?CLIENT_SERVER, {mkdir, CDir, UserName}) of
	{atomic, ok} ->
	    ok;
	{error, _} ->
	    {error, enoent}
    end.


%% --------------------------------------------------------------------
%% Function: mkdir/1 ->  ok | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
deldir(Dir) ->
    UserName = any,
    CDir = rm_slash(Dir),
    case gen_server:call(?CLIENT_SERVER, {delete, CDir, UserName}) of
	{ok, _} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.


%% --------------------------------------------------------------------
%% Function: chmod/4 ->  ok | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
chmod(FileName, UserName, UserType, CtrlACL) ->
    CFName = rm_slash(FileName),
    gen_server:call(?CLIENT_SERVER, {chmod, CFName, UserName, UserType, CtrlACL}).		
	
%% --------------------------------------------------------------------
%% Function: pread/3 ->  {ok, Data} | eof | {error, Reason} 
%% Description: read Size bytes from Offset 
%% --------------------------------------------------------------------	
pread(WorkerPid, Offset, Size) ->
    Len = min(Size, ?STRIP_SIZE),
    case gen_server:call(WorkerPid, {read, Len}) of
	{ok, Data} ->
	    loop_read([Data | []], WorkerPid, Offset + Len, Size - Len);
	eof ->
	    eof;
	{error, Reason} ->
	    {error, Reason}
    end.

loop_read(List, WorkerPid, Begin, Size) when Size > 0 ->
    Len = min(Size, ?STRIP_SIZE),
    case gen_server:call(WorkerPid, {read, Len}) of
	{ok, Part} ->
	    NewList= [Part | List],
	    loop_read(NewList, WorkerPid, Begin + Len, Size - Len);
	eof ->
	    Tmp = lists:reverse(List),
	    Data = list_to_binary(Tmp),
	    {ok, Data};
	{error, Reason} ->
	    {error, Reason}
    end;
loop_read(List, _, _, _) ->
    Tmp = lists:reverse(List),
    Data = list_to_binary(Tmp),
    {ok, Data}.

%% --------------------------------------------------------------------
%% Function: pwrite/3 ->  ok | {error, Reason} 
%% Description: write Data begin at Offset 
%% --------------------------------------------------------------------
pwrite(WorkerPid, _Offset, Data) ->
   gen_server:call(WorkerPid, {write, Data}). 

%% --------------------------------------------------------------------
%% Function: move/2 ->  ok | {error, Reason} 
%% Description: move file and dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
move(Src, Dst) ->
    UserName = any,
    CSrc = rm_slash(Src),
    CDst = rm_slash(Dst),
    gen_server:call(?CLIENT_SERVER, {move, CSrc, CDst, UserName}).

%% --------------------------------------------------------------------
%% Function: copy/2 ->  ok | {error, Reason} 
%% Description: copy file and dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
copy(Src, Dst) ->
    UserName = any,
    CSrc = rm_slash(Src),
    CDst = rm_slash(Dst),
    gen_server:call(?CLIENT_SERVER, {copy, CSrc, CDst, UserName}).
