%% Author: zyb@fit
%% Created: 2008-12-22
%% Description: TODO: Add description to util
-module(util).




%%
%% Include files
%%
-compile(export_all).
%%
%% Exported Functions
%%
%%-export([for/3]).


test()->
    fun acl:start/0.

%%
%% API Functions
%%

for(Max, Max, F) -> [F(Max)];
for(I, Max, F)   -> [F(I)|for(I+1, Max, F)].

%%
%% Local Functions
%%

for_test() ->    
    util:for(1,10,fun(I)->I*I end).


getNthList(Max, Max, List) ->  [Head | _] = List,
                               Head;
getNthList(I, Max, List)   ->  [_| Tail] = List,
                                    getNthList(I+1, Max, Tail).


c()->
    io:format("compiling file : metaDB~n"),
   	c:c(metaDB),
    io:format(".....ok~n"),
    io:format("compiling file : metagenserver~n"),
   	c:c(metagenserver),
    io:format(".....ok~n"),
    io:format("compiling file : metaserver~n"),
   	c:c(metaserver),
	io:format(".....ok~n"),
	io:format("compile finished.").

