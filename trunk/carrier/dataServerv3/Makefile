.SUFFIXES: .erl .beam .yrl

ERL = erl -boot start_clean -config elog.config -setcookie abc 

all: compile

compile: 
	erl -make

start-master:compile
	${ERL} -sname edfs_master -s edfs_master start

start-slave:compile
	${ERL} -sname edfs_slave -s edfs_datanode_app start

start-slave2:compile
	${ERL} -sname edfs_slave2 -s edfs_datanode_app start

start-app:compile
	${ERL} -sname lt -s ping_server ping -s app_data_server start 

start-sup:compile
	${ERL} -sname lt -s ping_server ping -s supervisor_data_server start_in_shell 

start-ds:compile
	${ERL} -sname lt -s ping_server ping -s data_gen_server start 

start-dsds:compile
	${ERL} -sname ltlt -s ping_server ping -s data_gen_server start 

start:compile
	${ERL} -sname client -s ping_server ping 

clean:
	rm -rf *.beam erl_crash.dump
	rm -rf test


