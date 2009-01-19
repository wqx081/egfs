%tables  record to create table.

-record(chunkmeta, {chunk_id, file_id, path, length, create_time, modify_time}).
-record(garbageinfo, {chunk_id, insert_time}).

-record(file_context, {file_id, file_size, chunk_index, chunk_id, nodelist,
		       socket, timestamp, pid}).
-record(read_context, {file_id, file_size, chunk_index, chunk_id, nodelist,
		       socket, timestamp, pid}).
-record(write_context, {file_id, file_size, chunk_index, chunk_id, nodelist,
		       socket, timestamp, pid}).


-define(SERVER_NAME, data_server).
-define(DATA_PORT, 7070).
-define(STRIP_SIZE, 8192).
-define(TOTAL_SPACE, 21474836480).
-define(INET_OP, [binary, {packet, 2}, {active, true}]). 

-define(GARBAGE_AUTO_COLLECT_PERIOD,	7000).    % 5000 milisecond = 5 second
-define(BOOT_REPORT_RETRY_PERIOD,	7000).    % 5000 milisecond = 5 second
-define(HEART_BEAT_REPORT_WAIT_TIME,	7000).    % 5000 milisecond = 5 second
-define(GARBAGE_AUTO_COLLECT_WAIT_TIME,	7000).    % 5000 milisecond = 5 second

