{application, app_meta_server,
    [{description, "meta server for egfs"},
     {vsn, "1.0"},
     {modules, [app_meta_server, supervisor_meta_server,
		meta_server,
		meta_db, ping_server, meta_util]},
     {registered, [supervisor_meta_server]},
     {applications, [kernel, stdlib]},
     {mod, {app_meta_server, []}},
     {start_phases, []}
]}.

