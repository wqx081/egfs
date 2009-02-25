# leave these lines alone
.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W $<

all: compile

compile: ${MODS:%=%.beam} 
		erl -make		

# remove all the code

clean:	
	rm -rf *.beam erl_crash.dump

