%% Author: zyb@fit
%% Created: 2008-12-22
%% Description: TODO: Add description to util
-module(util).

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

