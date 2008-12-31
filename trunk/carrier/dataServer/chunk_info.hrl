%tables  record to create table.

-record(chunkmeta, {chunk_id, file_id, path, length, create_time, modify_time}).


-define(SERVER_NAME, data_server1).
-define(TOTAL_SPACE, 21474836480).
