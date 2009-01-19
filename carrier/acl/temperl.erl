%handle_call({new, FileName, UserName}, {_From, _}, Tabacl) ->
%    Reply = case ets:lookup(Tabacl, FileName) of
%	[] ->
%	    lookup_acltab(Tabacl, new, FileName, UserName);
%	[Object] ->
%	    io:format("[Acl:~p]return is:~p~n",[?LINE, Object]),
%	    {FileName, file_has_exist}
%	    end,
%    {reply, Reply, Tabacl};



handle_call({ls, FolderName, _Mode, UserName}, _From, Tab) ->
   io:format("[Acl:~p]you want to list a folder:~p~n",[?LINE, FolderName]),
    Reply = case ets:lookup(Tab, FolderName) of
	[] ->
	    {error, {folder_does_not_exit}};
	[_] ->
	    %查询看username是否可删该file，并返回结果
	    look_up_ets(Tab, ls, FolderName, UserName)
	    end,
    {reply, Reply, Tab};

handle_call({delfolder, FolderName, _NewName, UserName}, _From, Tab) ->
    io:format("[Acl:~p]you want to delete a folder:~p~n",[?LINE, FolderName]),
    Reply = case ets:lookup(Tab, FolderName) of
	[] ->
	    {error, {folder_does_not_exit}};
	[_] ->
	    %查询看username是否可删该file，并返回结果
	    look_up_ets(Tab, delfolder, FolderName, UserName)
	    end,
    {reply, Reply, Tab};


handle_call({rename_folder, FolderName, _NewName, UserName}, _From, Tab) ->
    io:format("[Acl:~p]you want to rename a folder:~p~n",[?LINE, FolderName]),
    Reply = case ets:lookup(Tab, FolderName) of
	[] ->
	    {error, {folder_does_not_exit}};
	[_] ->
	    %查询看username是否可删该file，并返回结果
	    look_up_ets(Tab, renamefolder, FolderName, UserName)
	    end,
    {reply, Reply, Tab}
