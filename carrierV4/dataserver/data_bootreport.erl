-module(data_bootreport).
-include("../include/header.hrl").
-export([bootreport/0]).

bootreport() ->
    {ok, HostName}= inet:gethostname(),
    ChunkList = data_db:select_chunklist_from_chunkmeta(),
    io:format("Host:~p, ChunkList:~p~n",[HostName, ChunkList]).
    %gen_server:cast(?META_SERVER, {bootreport, HostName, ChunkList}).
	

