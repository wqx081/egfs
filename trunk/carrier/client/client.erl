%%%-------------------------------------------------------------------
%%% File    : client.erl
%%% Author  : xuchuncong <xuchuncong@gmail.com>
%%% Description : the client template:offer open/write/read/del function
%%%
%%% Created :  17 dec 2008 by xuchuncong 
%%%-------------------------------------------------------------------
-module(client).

-behaviour(gen_server).
-export([start/0]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).
-compile(export_all).


start() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
close()  -> gen_server:call(?MODULE, stop).

open(FileName)      -> gen_server:call(?MODULE, {open, FileName}).

write(FileName, Amount)  -> gen_server:call(?MODULE, {write, FileName, Amount}).

read(FileName)  -> gen_server:call(?MODULE, {read, FileName}).

del(FileName) -> gen_server:call(?MODULE, {del,FileName}).



init([]) -> {ok, ets:new(?MODULE,[])}.

handle_call({open,FileName}, _From, Tab) ->
    Reply = case ets:lookup(Tab, FileName) of
		[]  -> ets:insert(Tab, {FileName,0}), 
		       {you_have_openned_a_file, FileName};
		[_] -> {FileName, the_file_have_been_openned}
	    end,
    {reply, Reply, Tab};    
handle_call({write,FileName,X}, _From, Tab) ->
    Reply = case ets:lookup(Tab, FileName) of
		[]  -> please_open_the_file;
		[{FileName,Balance}] ->
		    %%NewBalance = Balance + X,
		    ets:insert(Tab, {FileName, X}),
		    {thanks, FileName, you_write_to_the_file, X}	
	    end,
    {reply, Reply, Tab};
handle_call({read,FileName}, _From, Tab) ->
    Reply = case ets:lookup(Tab, FileName) of
		[]  -> please_open_the_file;
		[{FileName,Balance}] ->
		    %%NewBalance = Balance + X,
		    %%ets:insert(Tab, {FileName, NewBalance}),
		    {thanks, FileName, the_file_content_is, Balance}	
	    end,
    {reply, Reply, Tab};
handle_call({del,FileName}, _From, Tab) ->
    Reply = case ets:lookup(Tab, FileName) of
		[]  -> please_open_the_file;
		[{FileName,Balance}] ->
                    %%when X =< Balance ->
		    %%NewBalance = Balance - X,
		    ets:delete(Tab,FileName),
		    {thanks, FileName, your_have_deleted_the_file, Balance}	
		%%[{FileName,Balance}] ->
		%%    {sorry,FileName,you_only_have,Balance,in_the_bank}
	    end,
    {reply, Reply, Tab};
handle_call(stop, _From, Tab) ->
    {stop, normal, closed, Tab}.
    

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.


    
