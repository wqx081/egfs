%tables  record to create table.

-record(chunkmeta, {chunk_id, file_id, path, length, create_time, modify_time}).
-record(garbageinfo, {chunk_id, insert_time}).

-define(SERVER_NAME, data_server).
-define(STRIP_SIZE, 8192).
-define(TOTAL_SPACE, 21474836480).
-define(GARBAGE_AUTO_COLLECT_PERIOD,	70000).    % 5000 milisecond = 5 second
