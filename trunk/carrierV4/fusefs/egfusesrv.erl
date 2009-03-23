-module(egfusesrv).
-export([start_link/2, start_link/3]).
%-behaviour(fuserl).
-export([ code_change/3,
	  handle_info/2,
	  init/1,
	  terminate/2,
	  getattr/4,
	  setattr/7,
	  lookup/5,
	  create/7,
	  unlink/5,
	  open/5,
	  read/7,
	  write/7,
	  flush/5,
	  rename/7,
	  mkdir/6,
	  rmdir/5,
	  readdir/7
	  ]).

-include_lib("kernel/include/file.hrl").
-include_lib("fuserl.hrl").
-include("../include/header.hrl").

-record(egfsrv, {inodes, names, pids}).

%%-define(PREFIX, "/home/lt/erlangDev/fuse/fuserl-2.0.4/tests/eunit").

start_link(LinkedIn, Dir) ->
    start_link(LinkedIn, Dir, "").

start_link(LinkedIn, Dir, MountOpts) ->
    fuserlsrv:start_link(?MODULE, LinkedIn, MountOpts, Dir, [], []).

init([]) ->
    State = #egfsrv{ inodes = gb_trees:from_orddict([{ 1, []}]),
	             names = gb_trees:empty() 
		     pids = ets:new(workers, [])},
    {ok, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
handle_info(_Msg, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.

-define (ROOTATTR, #stat{ st_ino = 1,
                          st_mode = ?S_IFDIR bor 8#0555,
			  st_size = 4096,
			  st_nlink = 2}).

getattr(_, 1, _, State) ->
    {#fuse_reply_attr{ attr = ?ROOTATTR, attr_timeout_ms = 1000}, State};
getattr(_, X, _, State) ->
    %%io:format("[~p, ~p] ~p~n", [?MODULE, ?LINE, X]),
    case gb_trees:lookup(X, State#egfsrv.inodes) of
	{value, {Parent, Name}} ->
	    {Attr, NewState} = my_get_attr({Parent, Name}, State),
	    {#fuse_reply_attr{ attr = Attr, attr_timeout_ms = 1000}, NewState};
	none ->
	    {#fuse_reply_err{ err = enoent}, State}
    end.

%% BASE = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {8,0,0}}).
datetime_to_seconds(DateTime) ->
    Base = 62167248000,
    Now = calendar:datetime_to_gregorian_seconds(DateTime),
    Now - Base.

seconds_to_datetime(Seconds) ->
    Base = 62167248000,
    Now = Base + Seconds,
    calendar:gregorian_seconds_to_datetime(Now).

my_get_attr({Parent, Name}, State) ->
    LocalName = Parent ++ Name,
    {Ino, NewState} = make_inode({Parent, Name}, State),
    {ok, FileInfo} = clientlib:read_file_info(LocalName),
    case FileInfo#filemeta.type of
	directory ->
	    Mode = ?S_IFDIR bor 8#0555,
	    Size = 4096,
	    Nlink = 2;
	_ ->
	    Mode = ?S_IFREG bor 8#0644,
	    Size = FileInfo#filemeta.size,
	    Nlink = 1
    end,

    Attr = #stat{ st_ino = Ino, 
		  st_size = Size, 
                  st_mode = Mode,
	          st_atime = datetime_to_seconds(FileInfo#filemeta.atime),
	          st_mtime = datetime_to_seconds(FileInfo#filemeta.mtime),
	          st_ctime = datetime_to_seconds(FileInfo#filemeta.ctime),
		  st_uid = FileInfo#filemeta.uid,
		  st_gid = FileInfo#filemeta.gid,
	          st_nlink = Nlink},
    {Attr, NewState}.

make_inode(GFName, State) ->
    case gb_trees:lookup(GFName, State#egfsrv.names) of
	{value, Ino} ->
	    {Ino, State};
	none ->
	    Inodes = State#egfsrv.inodes,
	    {Max, _} = gb_trees:largest(Inodes),
	    NewInodes = gb_trees:insert(Max + 1, GFName, Inodes),
	    Names = State#egfsrv.names,
	    NewNames = gb_trees:insert(GFName, Max+1, Names),
	    {Max+1, State#egfsrv{inodes = NewInodes, names = NewNames}}
    end.

lookup(_, X, BName, _, State) ->
    %%io:format("[~p, ~p] ~p, ~p~n", [?MODULE, ?LINE, X, BName]),
    Parent = get_parent(X, State),
    Name = binary_to_list(BName),
    {ok, All} = clientlib:listdir(Parent),
    case lists:member(Name, All) of
	true ->
	    {Ino, State1} = make_inode({Parent, Name}, State),
	    {Attr, NewState} = my_get_attr({Parent, Name}, State1),
	    {#fuse_reply_entry{
		fuse_entry_param = 
		    #fuse_entry_param{ ino = Ino,
			           generation = 1,
				   attr_timeout_ms = 1000,
				   entry_timeout_ms = 1000,
				   attr = Attr}},
	    NewState};
	false ->
	    {#fuse_reply_err{err = enoent}, State}
    end.
    
construct_worker(Path, Flags) ->
    case Flags band ?O_ACCMODE of
	?O_RDONLY ->
	    clientlib:open(Path, read);
	_->
	    clientlib:open(Paht, write)
    end.

get_worker(Hdl, Pids) ->
    case ets:lookup(Pids, Hdl) of
	[H|_] ->
	    {_, Pid} = H,
	    {ok, Pid};
	[] ->
	    {error, none}
    end.

open(_, X, Fi = #fuse_file_info{}, _, State) ->
    %%io:format("[~p, ~p] ~p~n", [?MODULE, ?LINE, X]),
    case gb_trees:lookup(X, State#egfsrv.inodes) of
	{value, {Parent, Name} } ->
	    LocalName = Parent ++ Name,
	    {ok, WorkerPid} = construct_worker(LocalName, Fi#fuse_file_info.flags),
	    Hdl = crypto:rand_bytes(8),
	    ets:insert(State#egfsrv.pids, {Hdl, WorkerPid}),

	    NFi = Fi#fuse_file_info{fh = Hdl},
	    {#fuse_reply_open{fuse_file_info = NFi}, State};
	none ->
	    {#fuse_reply_err{err = enoent}, State}
    end.
	

read(_, X, Size, Offset, _Fi, _, State) ->
    %%io:format("[~p, ~p] ~p ~p~n", [?MODULE, ?LINE, Size, Offset]),
    case gb_trees:lookup(X, State#egfsrv.inodes) of
	{value, {Parent, Name}} ->
	    LocalName = Parent ++ Name,
	    {ok, FileInfo} = clientlib:read_file_info(LocalName),
	    case FileInfo#filemeta.type of
		regular ->
		    Len = FileInfo#filemeta.size,
		    if 
			Offset < Len ->
			    {ok, IoDev} = get_worker(Fi#fuse_file_info.fh, State#egfsrv.pids),
			    %%{ok, IoDev} = clientlib:open(LocalName, read),
			    if 
				Offset + Size > Len ->
				    Take = Len - Offset,
				    {ok, Data} = clientlib:pread(IoDev, Offset, Take);
				true ->
				    {ok, Data} = clientlib:pread(IoDev, Offset, Size)
			    end,
			    %%clientlib:close(IoDev);
		    true ->
			Data = <<>>
		    end,
		    
		    NSize = erlang:size(Data),
		    {#fuse_reply_buf{buf = Data, size = NSize}, State};
		_ ->
		    {#fuse_reply_err{ err = eisdir}, State}
	    end;
	none ->
	    {#fuse_reply_err{ err = enoent}, State}
    end.

write(_, Inode, Data, Offset, _Fi, _, State) ->
    io:format("[~p, ~p] write~n", [?MODULE, ?LINE]),
    %%io:format("[~p, ~p] ~p~n", [?MODULE, ?LINE, Data]),
    case gb_trees:lookup(Inode, State#egfsrv.inodes) of
	{value, {Parent, Name}} ->
	    LocalName = Parent ++ Name,
	    {ok, FileInfo} = clientlib:read_file_info(LocalName),
	    case FileInfo#filemeta.type of
		regular ->
		    {ok, IoDev} = clientlib:open(LocalName, write),
		    ok = clientlib:pwrite(IoDev, Offset, Data),
		    clientlib:close(IoDev),
		    {#fuse_reply_write{count = erlang:size(Data)}, State};
		_ ->
		    {#fuse_reply_err{ err = eisdir}, State}
	    end;
	none ->
	    {#fuse_reply_err{ err = enoent}, State}
    end.

readdir(_, X, Size, Offset, _Fi, _, State) ->
    %%io:format("[~p, ~p] ~p, ~p, ~p~n", [?MODULE, ?LINE, X, Size, Offset]),
    {DotsEntryList, State1} = get_dots(X, State),
    {SubEntryList, NewState} = get_subs(X, State1),
    FullList = DotsEntryList ++ SubEntryList,

    Func = fun(E, {Total, Max}) ->
	    Cur = fuserlsrv:dirent_size(E),
	    if
		Total + Cur =< Max ->
		    {continue, {Total + Cur, Max}};
		true ->
		    stop
	    end
	end,

    DirEntryList = take_while(Func,
	    {0, Size},
	    lists:nthtail(Offset, FullList)),

    %%{ #fuse_reply_err{ err = enoent}, NewState}.
    { #fuse_reply_direntrylist{ direntrylist = DirEntryList}, NewState}.

take_while (_, _, []) -> 
  [];
take_while (F, Acc, [ H | T ]) ->
  case F (H, Acc) of
    { continue, NewAcc } ->
      [ H | take_while (F, NewAcc, T) ];
    stop ->
      []
  end.

get_dots(1, State) ->
    {[ #direntry{ name = ".", offset = 1, stat = ?ROOTATTR },
      #direntry{ name = "..", offset = 2, stat = ?ROOTATTR }], State};
get_dots(X, State) ->
    case gb_trees:lookup(X, State#egfsrv.inodes) of
	{value, {Parent, Name}} ->
	    {Attr, State1} = my_get_attr({Parent, Name}, State),
	    Dot = #direntry{name = ".", offset = 1, stat = Attr},

	    case Parent of
		"/" ->
		    NewState = State1,
		    DotDot = #direntry{name = "..", offset = 2, stat = ?ROOTATTR};
		_ ->
		    {PParent, PName} = split_dir(Parent),
		    {PAttr, NewState} = my_get_attr({PParent, PName}, State1),
		    DotDot = #direntry{name = "..", offset = 2, stat = PAttr}
	    end,

	    {[Dot, DotDot], NewState};
	none ->
	    get_dots(1, State)
    end.

%%split path like: "/home/lt/good/" into {/home/lt/, good}
split_dir(Path) ->
    Temp = filename:dirname(Path),
    Parent = filename:dirname(Temp) ++ "/",
    Name = filename:basename(Temp),
    {Parent, Name}.

%% 0 just for test
get_subs(0, State) ->
    {[#direntry{ name = "hello", offset = 3, stat = #stat{ st_ino = 2, st_mode = ?S_IFREG bor 8#444 } },
      #direntry{ name = "hello2", offset = 4, stat = #stat{ st_ino = 3, st_mode = ?S_IFREG bor 8#444}}], State};
get_subs(1, State) ->
    Parent = "/",
    {ok, Dirs} = clientlib:listdir(Parent),
    {DirsAttr, NewState} = get_dirs_attr(2, Parent, Dirs, [], State),
    {DirsAttr, NewState};
get_subs(X, State) ->
    case gb_trees:lookup(X, State#egfsrv.inodes) of
	{value, {PParent, PName}} ->
	    Parent = PParent ++ PName ++ "/",
	    {ok, Dirs} = clientlib:listdir(Parent),
	    {DirsAttr, NewState} = get_dirs_attr(2, Parent, Dirs, [], State),
	    {DirsAttr, NewState};
	none ->
	    none
    end.

get_dirs_attr(_, _Parent, [], Attrs, State) ->
    {lists:reverse(Attrs), State};
get_dirs_attr(N, Parent, [H|T], Attrs, State) ->
    %%io:format("[~p, ~p] ~p~n", [?MODULE, ?LINE, N]),
    {Attr, NewState} = my_get_attr({Parent, H}, State),
    NN = N + 1,
    Entry = #direntry{ name = H, offset = NN, stat = Attr},
    NewAttrs = [Entry | Attrs],
    get_dirs_attr(NN, Parent, T, NewAttrs, NewState).
    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_parent(1, _State) ->
    "/";
get_parent(Ino, State) ->
    {value, {PParent, PName}} = gb_trees:lookup(Ino, State#egfsrv.inodes),
    PParent ++ PName ++ "/".

create(_, PIno, BName, _Mode, Fi, _, State) ->
    io:format("[~p, ~p] ~p ~p~n", [?MODULE, ?LINE, PIno, BName]),
    Parent = get_parent(PIno, State),
    Name = binary_to_list(BName),
    FullPath = Parent ++ Name,
    {ok, Io} = clientlib:open(FullPath, write),
    clientlib:close(Io),
    {Attr, NewState} = my_get_attr({Parent, Name}, State), 
    {#fuse_reply_create{
	fuse_file_info = Fi,
	fuse_entry_param = #fuse_entry_param{ 
	    ino = Attr#stat.st_ino,
	    generation = 1,
	    attr_timeout_ms = 1000,
	    entry_timeout_ms = 1000,
	    attr = Attr}},
     NewState}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
try_set_mod(Attr, OldAttr, Toset) 
    when (Toset band ?FUSE_SET_ATTR_MODE) =/= 0 ->
	OldAttr#stat{st_mode = Attr#stat.st_mode};
try_set_mod(_, OldAttr, _) ->
    OldAttr.

try_set_uid(Attr, OldAttr, Toset) 
    when (Toset band ?FUSE_SET_ATTR_UID) =/= 0 ->
	OldAttr#stat{st_uid = Attr#stat.st_uid};
try_set_uid(_, OldAttr, _) ->
    OldAttr.

try_set_gid(Attr, OldAttr, Toset) 
    when (Toset band ?FUSE_SET_ATTR_GID) =/= 0 ->
        OldAttr#stat{st_gid = Attr#stat.st_gid};
try_set_gid(_, OldAttr, _) ->
    OldAttr.

try_set_size(Attr, OldAttr, Toset) 
    when (Toset band ?FUSE_SET_ATTR_SIZE) =/= 0 ->
        OldAttr#stat{st_size = Attr#stat.st_size};
try_set_size(_, OldAttr, _) ->
    OldAttr.

try_set_atime(Attr, OldAttr, Toset) 
    when (Toset band ?FUSE_SET_ATTR_ATIME) =/= 0 ->
        OldAttr#stat{st_atime = (Attr#stat.st_atime)};
try_set_atime(_, OldAttr, _) ->
    OldAttr.

try_set_mtime(Attr, OldAttr, Toset) 
    when (Toset band ?FUSE_SET_ATTR_MTIME) =/= 0 ->
        OldAttr#stat{st_mtime = (Attr#stat.st_mtime)};
try_set_mtime(_, OldAttr, _) ->
    OldAttr.

my_set_attr({Parent, Name}, NewAttr) ->
    _LocalName = Parent ++ Name,
    _FileInfo = #file_info{
		    mode = NewAttr#stat.st_mode,
		    size = NewAttr#stat.st_size,
		    atime = seconds_to_datetime(NewAttr#stat.st_atime),
		    mtime = seconds_to_datetime(NewAttr#stat.st_mtime),
		    uid = NewAttr#stat.st_uid,
		    gid = NewAttr#stat.st_gid},
    io:format("[~p, ~p]setatt not implemented.~n", [?MODULE, ?LINE]).
    %%file:write_file_info(FullName, FileInfo).

setattr(_, Ino, Attr, Toset, _Fi, _, State) ->
    %%io:format("[~p, ~p] ~p ~p~n", [?MODULE, ?LINE, Ino, Attr]),
    case gb_trees:lookup(Ino, State#egfsrv.inodes) of
	{value, {Parent, Name}} ->
	    {OldAttr, NewState} = my_get_attr({Parent, Name}, State),
	    NewAttr1 = try_set_mod(Attr, OldAttr, Toset), 
	    NewAttr2 = try_set_uid(Attr, NewAttr1, Toset),
	    NewAttr3 = try_set_gid(Attr, NewAttr2, Toset),
	    NewAttr4 = try_set_size(Attr, NewAttr3, Toset),
	    NewAttr5 = try_set_atime(Attr, NewAttr4, Toset),
	    NewAttr  = try_set_mtime(Attr, NewAttr5, Toset),
	    
	    my_set_attr({Parent, Name}, NewAttr),
	    {#fuse_reply_attr{attr = NewAttr, attr_timeout_ms = 1000}, NewState};
	none ->
	    {#fuse_reply_err{err = enoent}, State}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
delete_info({Parent, Name}, State) ->
    case gb_trees:lookup({Parent, Name}, State#egfsrv.names) of
	{value, Ino} ->
	    Inodes = State#egfsrv.inodes,
	    NewInodes = gb_trees:delete_any(Ino, Inodes),
	    Names = State#egfsrv.names,
	    NewNames = gb_trees:delete_any({Parent, Name}, Names),
	    #egfsrv{inodes = NewInodes, names = NewNames};
	none ->
	    State
    end.

unlink(_, PIno, BName, _, State) ->
    %%io:format("[~p, ~p] ~p ~p~n", [?MODULE, ?LINE, PIno, BName]),
    Name = binary_to_list(BName),
    Parent = get_parent(PIno, State),
    LocalName = Parent ++ Name,
    case clientlib:delete(LocalName) of
	ok ->
	    NewState = delete_info({Parent, Name}, State),
	    {#fuse_reply_err{ err = ok}, NewState};
	{error, Reason} ->
	    {#fuse_reply_err{ err = Reason}, State}
    end.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
mkdir(_, PIno, BName, _Mode, _, State) ->
    %%io:format("[~p, ~p] ~p ~p~n", [?MODULE, ?LINE, PIno, BName]),
    Name = binary_to_list(BName),
    Parent = get_parent(PIno, State),
    LocalName = Parent ++ Name,
    case clientlib:mkdir(LocalName) of
	ok ->
	    {Attr, NewState} = my_get_attr({Parent, Name}, State), 

	    {#fuse_reply_entry{ 
		fuse_entry_param = #fuse_entry_param{
		    ino = Attr#stat.st_ino,
		    generation = 1,
		    attr = Attr,
		    attr_timeout_ms = 1000,
		    entry_timeout_ms = 1000}}, 
	     NewState};
	{error, Reason} ->
	    {#fuse_reply_err{ err = Reason}, State}
    end.
				
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
rmdir(_, PIno, BName, _, State) ->
    %%io:format("[~p, ~p] ~p ~p~n", [?MODULE, ?LINE, PIno, BName]),
    Name = binary_to_list(BName),
    Parent = get_parent(PIno, State),
    LocalName = Parent ++ Name,
    %%FullName = ?PREFIX ++ LocalName, %%%%%%%%%THE TRICKY PREFIX%%%%%%%
    case clientlib:deldir(LocalName) of
	ok ->
	    NewState = delete_info({Parent, Name}, State),
	    {#fuse_reply_err{ err = ok }, NewState};
	{error, Reason} ->
	    {#fuse_reply_err{ err = Reason}, State}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
flush(_, _Ino, _Fi, _, State) ->
    %%io:format("[~p, ~p] ~p ~n", [?MODULE, ?LINE, Ino]),
    {#fuse_reply_err{ err = ok }, State}.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
rename(_, PIno, BName, NPIno, BNewName, _, State) ->
    %%io:format("[~p, ~p] (~p,~p) -> (~p,~p)~n", [?MODULE, ?LINE, PIno, BName, NPIno, BNewName]),
    Name = binary_to_list(BName),
    NewName = binary_to_list(BNewName),
    Parent = get_parent(PIno, State),
    NParent = get_parent(NPIno, State),
    FullName = Parent ++ Name,
    NewFullName = NParent ++ NewName,
    case clientlib:rename(FullName, NewFullName) of
	ok ->
	    State1 = delete_info({Parent, Name}, State),
	    {_, NewState} = make_inode({NParent, NewName}, State1),
	    {#fuse_reply_err{ err = ok }, NewState};
	{error, Reason} ->
	    {#fuse_reply_err{ err = Reason}, State}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%crypto:rand_uniform(1,18446744073709551616),
