-module(client_tests).
-include("../include/header.hrl").
-compile(export_all).

-define(FILELIST, "./testfiles/Zfilelist.txt").
-define(INPUTDIR, "./testfiles/").
-define(OUTPUTDIR, "./outfiles/").

 
ping(Host) ->
	net_adm:world_list(Host),
	timer:sleep(timer:seconds(200)).

generate_files() ->
	file:make_dir(?INPUTDIR),
	file:make_dir(?OUTPUTDIR),	
%    generate_file(2, 1024),
%    generate_file(2, 1048576),
%    generate_file(2, 104857600). 
%	generate_file(1, 2097152),    
%	generate_file(1, 20971520), 	
%	generate_file(1, 209715200).
	generate_file(100, 10240). 

generate_file(0, _) ->
	ok;
generate_file(FileNum, FileSize) ->
    {ok, ListHdl} = file:open(?FILELIST, [raw,append]), 
    FileName = lib_uuid:to_string(lib_uuid:gen()),
    FilePath= ?INPUTDIR ++ FileName,
    {ok, Hdl} = file:open(FilePath, [raw,write]),	
    loop_gen_write(FileSize,Hdl),
    {ok,MD5}=lib_md5:file(FilePath),
    NewFileName="/"++FileName,
    Str=io_lib:format("{~p,~p,~p}.~n",[NewFileName,FileSize,MD5]),
	file:write(ListHdl, Str),   
	file:close(ListHdl),	 
	generate_file(FileNum-1, FileSize).  

loop_gen_write(0, Hdl) ->
	file:close(Hdl);
loop_gen_write(FileSize, Hdl) ->
	Num = lists:min([FileSize, ?STRIP_SIZE]),
	Bin = gen_bin(Num),
	file:write(Hdl, Bin),
	loop_gen_write(FileSize-Num, Hdl).
	    
gen_bin(FileSize) ->
	{A1,A2,A3}=now(),
	random:seed(A1,A2,A3), 
    gen_bin(random:uniform(math:pow(2, 48)) - 1, random:uniform(math:pow(2, 12)) - 1, random:uniform(math:pow(2, 32)) - 1, random:uniform(math:pow(2, 30)) - 1, FileSize*8).
gen_bin(R1, R2, R3, R4, FileSize) ->
	Length =FileSize div 4,
    <<R1:Length, R2:Length, R3:Length, R4:Length>>.    


%% --------------------------------------------------------------------
%% test function
%% --------------------------------------------------------------------
test_write_all() ->
	statistics(wall_clock),
	{ok, FileData}= file:consult(?FILELIST),
	lists:foreach(fun testw/1, FileData),
	{_,Time}=statistics(wall_clock),
	error_logger:info_msg("[~p, ~p]: Write Process Total Time= ~p microseconds~n", [?MODULE, ?LINE, Time]).
	
test_read_all() ->
	statistics(wall_clock),
	{ok, FileData}= file:consult(?FILELIST),
	lists:foreach(fun testr/1, FileData),
	{_,Time}=statistics(wall_clock),
	error_logger:info_msg("[~p, ~p]: Read Process Total Time= ~p microseconds~n", [?MODULE, ?LINE, Time]).
			
	
testw({FileName,_FileSize, _MD5}) ->
	case gen_server:call(client_server, {open,FileName,write, any}) of
		{ok, ClientWorkerPid}  ->
			{ok,Hdl}=file:open(?INPUTDIR++FileName,[binary,raw,read,read_ahead]),
			write_loop(ClientWorkerPid, Hdl),
			gen_server:call(ClientWorkerPid,{close});
		{error, Why} ->
			io:format("Error:~p~n",[Why]),
			exit(normal)
	end.
	
write_loop(ClientWorkerPid, Hdl)->
	case file:read(Hdl,?STRIP_SIZE) of % read 128K every time 
		{ok, Data} ->
			gen_server:call(ClientWorkerPid,{write,Data},120000),
			write_loop(ClientWorkerPid, Hdl);
		eof ->
			file:close(Hdl);	
		{error,Reason} ->
			Reason
	end.	

testr({FileName,_FileSize, MD5}) ->
	TargetFile = "./outfiles"++FileName,
	case gen_server:call(client_server,{open,FileName, read, any}) of
		{ok, ClientWorkerPid}   ->
			{ok,Hdl}=file:open(TargetFile, [binary,raw,write]),
			read_loop(ClientWorkerPid,Hdl),
			gen_server:call(ClientWorkerPid,{close});
		{error, Why} ->
			io:format("Error:~p~n",[Why]),
			exit(normal)
	end,
	{ok,NewMD5}=lib_md5:file(TargetFile), 
	case NewMD5 =:= MD5 of
		true ->
			io:format("Right:read ~p correctly~n",[FileName]);
		false ->
			io:format("Error:read ~p error~n",[FileName]),
			exit(normal)
	end.
	
read_loop(ClientWorkerPid, Hdl) ->
	case  gen_server:call(ClientWorkerPid,{read,?STRIP_SIZE},120000) of % read 128K every time 
		{ok, Data} ->
			file:write(Hdl,Data),
			read_loop(ClientWorkerPid, Hdl);
		eof ->
			file:close(Hdl);
		{error,Reason} ->
			Reason
	end.

