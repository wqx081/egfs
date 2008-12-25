-module(toolkit).
-include_lib("kernel/include/file.hrl").
-export([get_file_size/1,
	 get_file_handle/2,
	 get_file_name/1,
	 rm_pending_chunk/1,
	 report_metaServer/4]).
-compile(export_all).

-define(DATA_HOME, "./data").

get_file_size(FileName) ->
    case file:read_file_info(FileName) of
	{ok, Facts} ->
	    {ok, Facts#file_info.size};
	Other ->
	    Other
    end.

get_file_handle(read, ChunkID) when is_binary(ChunkID) ->
    {ok, Name} = get_file_name(ChunkID),
    Result = file:open(Name, [binary, raw, read, read_ahead]),
    Result;
get_file_handle(write, ChunkID) when is_binary(ChunkID) ->
    {ok, Name} = get_file_name(ChunkID),
    %% file:delete(Name),
    Result = file:open(Name, [binary, raw, append]),
    Result;
get_file_handle(_, _) ->
    {error, "ChunkID_is_not_a_binary"}.
    

get_file_name(ChunkID) when is_binary(ChunkID) ->
    <<Fn:48, D1:4, D2:4, D3:4, D4:4>> = ChunkID,
    PARTS = [D1, D2, D3, D4],
    R = lists:map(fun(X) -> integer_to_list(X) end, PARTS),
    PATH = generate_dirs("", [?DATA_HOME | R]),
    Name = lists:append([PATH, integer_to_list(Fn)]),
    {ok, Name};
get_file_name(_) ->
    {error, "ChunkID_is_not_a_binary"}.

generate_dirs(Cur, [H|T]) ->
    Cur2 = lists:append([Cur, H, "/"]),
    Bool = filelib:is_dir(Cur2),
    if
	not(Bool) ->
	    file:make_dir(Cur2);
	true ->
	    void
    end,
    generate_dirs(Cur2, T);
generate_dirs(Cur, []) ->
    Cur.

rm_pending_chunk(ChunkID) ->
    io:format("[dataserver]: chuunk should be removed ~p~n", ChunkID),
    {ok, "has removed it"}.

report_metaServer(FileID, _ChunkIndex, ChunkID, Len) ->
    io:format("[dataserver]: reporting to metaserver (~p, ~p)~n", [ChunkID, Len]),
    gen_server:call({global, metagenserver}, {registerchunk, FileID, ChunkID, Len, []}),
    {ok, "has reported it"}.

