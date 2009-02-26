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

cl1:compile
	${ERL} -pa ../lib -sname  ${HOSTNAME}client1 -s lib_ping ping ${METASERVER} -s client start_link 
	
cl2:compile
	${ERL} -pa ../lib -sname  ${HOSTNAME}client1 -s lib_ping ping ${METASERVER} -s client start_link 

cl3:compile
	${ERL} -pa ../lib -sname  ${HOSTNAME}client1 -s lib_ping ping ${METASERVER} -s client start_link 

# remove all the code

clean:	
	rm -rf *.beam erl_crash.dump

