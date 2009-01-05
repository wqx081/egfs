%% Author: zyb@fit
%% Created: 2008-12-25
%% Description: TODO: Add description to start
-module(start).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([startMeta/0]).

%%
%% API Functions
%%

startMeta()->    
    c:cd("D:/EclipseWorkS/edu.tsinghua.carrier/carrier/metaServer"),   %%TODO.
   	c:c(metaDB),
   	c:c(metagenserver),
   	c:c(metaserver),
	metagenserver:start(),
	net_adm:ping(xcc@xcc),
    net_adm:ping(llk@llk),
    net_adm:ping(lt@lt),
	net_adm:ping(cl@lt).


startClient()->
    asdf.




    
%%
%% Local Functions
%%

