%tables  record to create table.

-record(chunkmeta, {chunk_id, file_id, path, size, create_time, modify_time}).
-record(hostinfo, {ip, host, free_space, total_space}).


