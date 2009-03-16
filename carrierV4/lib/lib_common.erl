%%%-------------------------------------------------------------------
%%% File    : lib_common.erl
%%% Author  : Xiaomeng Huang
%%% Description : Metadata Server
%%%
%%% Created :  30 Jan 2009 by Xiaomeng Huang
%%%-------------------------------------------------------------------
-module(lib_common).
-include("../include/header.hrl").
-export([generate_processname/2,
		 get_local_ip/0,
		 get_file_handle/1,
		 get_file_name/1,
		 generate_dirs/2,
         for/3,
         idToAtom/2,
         get_rid_of_last_slash/1
        ]).



for(Max, Max, F) -> [F(Max)];
for(I, Max, F)   -> [F(I)|for(I+1, Max, F)].

idToAtom(Bin,Mode)->
    List = binary_to_list(Bin),
%%     ModePreFix = binary_to_list(term_to_binary(Mode)),
    case Mode of 
        r ->
            ModePreFix = $r;
        w ->
            ModePreFix = $w;
        _ ->
            ModePreFix = $?    
    end,    
    ModeList = lists:append([ModePreFix],List),
    list_to_atom(ModeList).

generate_processname(Filename,Mode)->
    error_logger:info_msg("lib_common:generate_processname_~p, ~p~n",[Filename,Mode]),
    ModePreFix= case Mode of 
					read -> r@@@;
					write -> w@@@;
					append -> a@@@		    
				end,    
    ModeList = lists:append(atom_to_list(ModePreFix),Filename),
    list_to_atom(ModeList).

get_rid_of_last_slash(FileName)->
    case string:len(FileName) of
        1->
            FileName;
        Any->
            case string:right(FileName,1) =:="/" of
                true->
                    get_rid_of_last_slash(string:left(FileName,Any-1));
                _False->
                    FileName
            end
    end.
                    



get_local_ip() ->
    {ok, Host} = inet:gethostname(),
    {ok, IP} = inet:getaddr(Host, inet),
	{ok, IP}.
	
get_file_handle({read, ChunkID}) ->
    {ok, Name} = get_file_name(ChunkID),
    Result = file:open(Name, [binary, raw, read, read_ahead]),
    Result;
    
get_file_handle({write, ChunkID}) ->
    {ok, Name} = get_file_name(ChunkID),
    Result = file:open(Name, [binary, raw, write]),
    Result.
    
get_file_name(ChunkID) ->
    <<D1:4, D2:4, D3:4, D4:4, Fn:112>> = ChunkID,
    PARTS = [D1, D2, D3, D4],
    R = lists:map(fun(X) -> integer_to_list(X) end, PARTS),
	Root = ?DATA_PREFIX ++ atom_to_list(node()),
    PATH = generate_dirs("", [ Root | R]),
    Name = lists:append([PATH, integer_to_list(Fn)]),
    {ok, Name}.

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

