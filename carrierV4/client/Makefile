# leave these lines alone
.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W $<

HOSTNAME = $(shell hostname)

ERL = erl -boot start_sasl -config system -setcookie carrier 

all: compile

compile: ${MODS:%=%.beam} 
	erl -make	

## run an application from the makefile

cl1:compile
	${ERL} -pa ../lib -sname  ${HOSTNAME}client1 -s client_app start -s crypto start
	
cl2:compile
	${ERL} -pa ../lib -sname  ${HOSTNAME}client2 -s client_app start -s crypto start

cl3:compile
	${ERL} -pa ../lib -sname  ${HOSTNAME}client3 -s client_app start -s crypto start

# remove all the code

clean:	
	rm -rf *.beam erl_crash.dump

