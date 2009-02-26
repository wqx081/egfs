-module(data_md5check).
-include("../include/header.hrl").
-export([md5check/0]).

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
	case data_db:select_item_from_chunkmeta_id(ChunkID, MD5) of
		[] ->
			%file:delete(File);
			error_logger:info_msg("[~p, ~p]: delete file:~p ~n", [?MODULE, ?LINE, FilePath]),
			io:format("delete file:~p ~n",[FilePath])
	end.

getrootpath() ->
	"./Chunks." ++ atom_to_list(node()).
