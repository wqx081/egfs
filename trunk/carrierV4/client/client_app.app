{application, client_app,
    [{description, "client genserver for distributed file system---carrier"},
     {vsn, "1.0"},
     {modules, [client_app, client_supervisor, client_server,client_worker, client_tests]},
     {registered, [client, client_supervisor]},
     {applications, [kernel, stdlib, sasl]},
     {mod, {client_app, []}},
     {start_phases, []}
]}.

