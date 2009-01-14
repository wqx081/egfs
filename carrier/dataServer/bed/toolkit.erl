-module(toolkit).
-include("../include/egfs.hrl").
-include_lib("kernel/include/file.hrl").
-export([get_file_size/1,
	 get_file_handle/2,
	 get_file_name/1,
	 get_local_addr/0,
	 get_proc_name/3,
	 parse_config/2,
	 rm_pending_chunk/1,
	 report_metaServer/4]).
-compile(export_all).

-define(DATA_HOME, "./data").

start_listen() ->
    {ok, Listen} = gen_tcp:listen(?DATA_PORT, ?INET_OP),
    {ok, IP} = get_local_addr(),
    {ok, Listen, IP, ?DATA_PORT}.

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
    %%io:format("[dataserver]: reporting to metaserver (~p, ~p)~n", [ChunkID, Len]),
    gen_server:call(?META_SERVER, {registerchunk, FileID, ChunkID, Len, []}),
    {ok, "has reported it"}.

get_local_addr() ->
    {ok, Host} = inet:gethostname(),
    inet:getaddr(Host, inet).

get_proc_name(Op, CPid, ChunkID) ->
    Bin = term_to_binary({Op, CPid, ChunkID}),
    List = binary_to_list(Bin),
    _Atom = list_to_atom(List).

parse_config(service, ConfigFile) ->
    {ok, Config} = file:consult(ConfigFile),
    [Value] = [Value || {service, Value} <- Config],
    {ok, Value};
parse_config(total_space, ConfigFile) ->
    {ok, Config} = file:consult(ConfigFile),
    [Value] = [Value || {total_space, Value} <- Config],
    {ok, Value}.

timestamp() ->
    {H, M, S} = time(),
    _Result = H*3600 + M*60 + S.
