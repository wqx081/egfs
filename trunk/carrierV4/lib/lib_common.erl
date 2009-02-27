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
		 generate_dirs/2]).

generate_processname(Filename,Mode)->
    ModePreFix= case Mode of 
					r -> r@@@;
					w -> w@@@;
					a -> a@@@		    
				end,    
    ModeList = lists:append(atom_to_list(ModePreFix),Filename),
    list_to_atom(ModeList).

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

