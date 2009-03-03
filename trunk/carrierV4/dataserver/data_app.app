{application, data_app,
    [{description, "data server for distributed file system---carrier"},
     {vsn, "1.0"},
     {modules, [data_app, data_supervisor, data_server, data_worker]},
     {registered, [data_server, data_supervisor]},
     {applications, [kernel, stdlib, sasl]},
     {mod, {data_app, []}},
     {start_phases, []}
]}.

