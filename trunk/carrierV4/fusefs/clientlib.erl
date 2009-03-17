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
	  chmod/4]).

-define(CLIENT_SERVER, client_server).

min(A, B) ->
    if 
	A > B ->
	    B;
	true  ->
	    A
    end.

%%--------------------------------------------------------------------------------
%% Function: open(string(),Mode) -> {ok, ClientWorkerPid} | {error, Reason} 
%% 			 Mode = r | w 
%% Description: open a file according to the filename and open mode
%%--------------------------------------------------------------------------------
open(FileName, Mode) ->
    UserName = any,
    gen_server:call(?CLIENT_SERVER, {open, FileName, Mode, UserName}).

%% --------------------------------------------------------------------
%% Function: close/1 ->  ok | {error, Reason} 
%% Description: close file
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------
close(WorkerPid) ->
    gen_server:call(?CLIENT_SERVER, {close, WorkerPid}).
	
%% --------------------------------------------------------------------
%% Function: delete/1 ->  ok | {error, Reason} 
%% Description: delete file
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------
delete(FileName)->
    UserName = any,
    gen_server:call(?CLIENT_SERVER, {delete, FileName,UserName}).	

%% --------------------------------------------------------------------
%% Function: read_file_info/1 ->  ok | {error, Reason} 
%% Description: read file meta info 
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
read_file_info(FileName) ->
    UserName = any,
    gen_server:call(?CLIENT_SERVER, {getfileinfo, FileName, UserName}).	
	
%% --------------------------------------------------------------------
%% Function: listdir/1 ->  list() | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
listdir(Dir) ->
    UserName = any,
    gen_server:call(?CLIENT_SERVER, {list, Dir, UserName}).	
	
%% --------------------------------------------------------------------
%% Function: mkdir/1 ->  ok | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
mkdir(Dir) ->
    UserName = any,
    gen_server:call(?CLIENT_SERVER, {mkdir, Dir, UserName}).	


%% --------------------------------------------------------------------
%% Function: mkdir/1 ->  ok | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
deldir(Dir) ->
    UserName = any,
    gen_server:call(?CLIENT_SERVER, {delete, Dir, UserName}).	

%% --------------------------------------------------------------------
%% Function: chmod/4 ->  ok | {error, Reason} 
%% Description: list dir
%% Returns: ok | {error, Reason}
%% --------------------------------------------------------------------	
chmod(FileName, UserName, UserType, CtrlACL) ->
    gen_server:call(?CLIENT_SERVER, {chmod, FileName, UserName, UserType, CtrlACL}).		
	
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
    gen_server:call(?CLIENT_SERVER, {move, Src, Dst, UserName}).