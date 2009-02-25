# leave these lines alone
.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W $<

HOSTNAME = $(shell hostname)
METASERVER= ms@hxm

ERL = erl -boot start_sasl -config ../elog -setcookie carrier 

all: compile

compile: ${MODS:%=%.beam} 
	erl -make	

## run an application from the makefile

ds:compile
	${ERL} -pa ../lib -sname  ${HOSTNAME}_dataserver -s lib_ping ping ${METASERVER} -s data_app start 

# remove all the code

clean:	
	rm -rf *.beam erl_crash.dump

