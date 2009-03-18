%%%-------------------------------------------------------------------
%%% File    : lib_uuid.erl
%%% Author  : Xiaomeng Huang
%%% Description : Metadata Server
%%%
%%% Created :  30 Jan 2009 by Xiaomeng Huang
%%%-------------------------------------------------------------------
-module(lib_uuid).
-export([gen/0, to_string/1, test/1]).

gen() ->
    A1=crypto:rand_uniform(1, 4294967295),
    A2=crypto:rand_uniform(1, 4294967295),
    A3=crypto:rand_uniform(1, 4294967295),
    random:seed(A1, A2, A3),
    U = <<
    (random:uniform(4294967295)):32,
    (random:uniform(4294967295)):32,
    (random:uniform(4294967295)):32,
    (random:uniform(4294967295)):32
    >>,
    format_uuid(U, 4). 
format_uuid(<<TL:32, TM:16, THV:16, CSR:8, CSL:8, N:48, _Rest/binary>>, V) ->
    <<TL:32, TM:16, ((THV band 16#0fff) bor (V bsl 12)):16, ((CSR band 16#3f) bor 16#80):8, CSL:8, N:48>>.
to_string(<<TL:32, TM:16, THV:16, CSR:8, CSL:8, N:48>> = _UUID) ->
    lists:flatten(io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~2.16.0b~2.16.0b-~12.16.0b", [TL, TM, THV, CSR, CSL, N])).

test(N) ->
	Table=ets:new(a,[]),
	A=test(N,Table),
	io:format("Table=~p~n",[A]).	
%	io:format("length=~p~n",[length(A)]).
test(0,Table) ->
	Table;
test(N,Table) ->
	A=to_string(gen()),
	case ets:lookup(Table,A) of
		[_] ->
			io:format("error: the same uuid ~p  in the ets table~n",[A]);
		[]	->
			void
	end,
	ets:insert(Table,{A,0}),
	test(N-1,Table).    
