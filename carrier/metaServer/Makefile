.SUFFIXES: .erl .beam .yrl

ERL = erl -boot start_clean -config elog.config -setcookie test 

all: start

compile: 
	erl -make

once:compile
	-rm -rf Mnesia*
	${ERL} -sname zyb -s metaDB do_this_once -s init stop 

start:compile once
	${ERL} -sname zyb -s app_meta_server start

start-app:compile
	${ERL} -sname zyb -s app_meta_server start

start-sup:compile
	${ERL} -sname zyb -s supervisor_meta_server start_in_shell 

start-no-sup:compile
	${ERL} -sname zyb -s metagenserver start 

clean:
	rm -rf *.beam erl_crash.dump
	rm -rf test


