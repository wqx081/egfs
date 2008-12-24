%% Author: zyb@fit
%% Created: 2008-12-22
%% Description: TODO: Add description to util
-module(util).
-compile(export_all).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([for/3]).

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
