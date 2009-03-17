-module(data_timer).
-include("../include/header.hrl").
-export([heartbeat/0, bootreport/0, md5check/0]).

heartbeat() ->
	{ok, Hosts} = application:get_env(data_app, metaserver),
	{ok, WaitTime} = application:get_env(data_app, waittime),	
	net_adm:world_list(Hosts),
	timer:sleep(timer:seconds(WaitTime)),
    {ok, HostName}= inet:gethostname(),
	case gen_server:call(?HOST_SERVER, {heartbeat, list_to_atom(HostName), uplink}) of
		needreport ->
			gen_server:call(?HOST_SERVER, {register_dataserver, list_to_atom(HostName), node(), undefined, undefined, uplink}),
			bootreport();
		_Any ->
			void
	end.
			
bootreport() ->
    {ok, HostName}= inet:gethostname(),
    ChunkList = data_db:select_chunklist_from_chunkmeta(),
    case ChunkList of
    	[] ->
    		void;
    	_ ->
    		gen_server:cast(?META_SERVER, {bootreport,  list_to_atom(HostName), ChunkList})
	end.

md5check() ->
	Root = getrootpath(),
	md5check(Root).

md5check([]) ->
	void; 
md5check(Dir) ->
	case filelib:is_dir(Dir) of
		true ->
			checkdir({dir,Dir});
		false ->
			case filelib:is_file(Dir) of
				true ->
					checkdir({file,Dir})
			end	
	end.
	
checkdir({dir, Dir}) ->
	case file:list_dir(Dir) of 
		{ok,Filenames} ->
			Pathnames = lists:map(fun(X) -> Dir++"/"++X end, Filenames),
			lists:foreach(fun(X) -> md5check(X) end, Pathnames);
		{error, _} ->
			void
	end;
checkdir({file, FilePath}) ->
	{ok,MD5}=lib_md5:file(FilePath),
	Root = getrootpath()++"/",
	[D1,D2,D3,D4,Fn] = lists:map(fun(X) -> list_to_integer(X) end, filename:split(FilePath--Root)),
	ChunkID = <<D1:4, D2:4, D3:4, D4:4, Fn:112>>,
	case data_db:is_exsit_in_chunkmeta(ChunkID, MD5) of
		[] ->
			data_db:delete_chunkmeta_item(ChunkID),
			file:delete(FilePath),
			error_logger:info_msg("[~p, ~p]: delete file:~p ~n", [?MODULE, ?LINE, FilePath]),
			io:format("delete file:~p ~n",[FilePath])
	end.

getrootpath() ->
	?DATA_PREFIX ++ atom_to_list(node()).
