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

open(FileName)          -> gen_server:call(?MODULE, {open, FileName}).

write(FileName, Amount) -> gen_server:call(?MODULE, {write, FileName, Amount}).

read(FileName)          -> gen_server:call(?MODULE, {read, FileName}).

del(FileName)           -> gen_server:call(?MODULE, {del,FileName}).



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
                    readChunk(),
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


readChunk() ->
    {ok, Socket} = gen_tcp:connect(localhost, 9999, [binary, {packet, 2}, {active, true}]),
    Read_req = {read, 2000, 0, 1024},
    ok = gen_tcp:send(Socket, term_to_binary(Read_req)),

    process_flag(trap_exit, true),

    receive
	{tcp, Socket, Bin} ->
	    Response = binary_to_term(Bin),
	    io:format("Response:~p~n", [Response]),

	    case Response of
		{ok, Port} ->
		    _Child = spawn_link(fun() -> receive_data(localhost, Port) end);
		{error, _Why} ->
		    io:format("Read Req can't be satisfied~n")
	    end
    end.

receive_data(Host, Port) ->
    {ok, DataSocket} = gen_tcp:connect(Host, Port, [binary, {packet, 2}, {active, true}]),
    loop(DataSocket).

loop(DataSocket) ->
    receive
	{tcp, DataSocket, Data} ->
	    write(Data),
	    loop(DataSocket);
	{tcp_closed, DataSocket} ->
	    io:format("read chunk over!~n");
	{client_close, _Why} ->
	    io:format("client close the datasocket~n"),
	    gen_tcp:close(DataSocket)
    end.

write(Data) ->
    {ok, Hdl} = file:open("recv.dat", [raw, append, binary]),
    file:write(Hdl, Data),
    file:close(Hdl).

test_write() ->
    {ok, Hdl} = file:open("send.dat", [raw, read, binary]),
    {ok, Binary} = file:pread(Hdl, 5, 4),
    %%{ok, Binary} = file:read_file("send.dat"),
    write(Binary).


    
