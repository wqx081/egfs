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
