# leave these lines alone
.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W $<

HOSTNAME = $(shell hostname)

ERL = erl -boot start_sasl -config ../carrier -setcookie carrier 

all: compile

compile: ${MODS:%=%.beam} 
	erl -make	

## run an application from the makefile
db: 
	${ERL} -sname ${HOSTNAME}dataserver -s data_db do_this_once 

cleardb: 
	${ERL} -sname ${HOSTNAME}dataserver -s data_db clear_tables

ds:compile
	${ERL} -pa ../lib -sname  ${HOSTNAME}dataserver -s data_app start 

# remove all the code

clean:	
	rm -rf *.beam erl_crash.dump

