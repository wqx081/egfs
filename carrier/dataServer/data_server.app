{application, data_server,
    [{description, "data server for egfs"},
     {vsn, "1.0"},
     {modules, [app_data_server, supervisor_data_server,
		data_gen_server, data_worker,
		toolkit, ping_server, lib_misc]},
     {registered, [supervisor_data_server]},
     {applications, [kernel, stdlib]},
     {mod, {app_data_server, []}},
     {start_phases, []}
]}.

