ERL = erl
ERLFLAGS = -pa eunit/ebin -noshell -sname llk -setcookie abc 

ERLC = erlc 
ERLCFLAGS = -pa eunit/ebin

SUB_DIRECTORIES = eunit

.SUFFIXES: .erl .beam .yrl

.erl.beam:
	$(ERLC) $(ERLCFLAGS) -W $<

.yrl.erl:
	$(ERLC) $(ERLCFLAGS) -W $<

all: subdirs compile test

compile: eunit client.beam fib.beam client_tests.beam

client.beam:../client/client.erl
	$(ERLC) $(ERLCFLAGS) ../client/client.erl

subdirs:
	@for d in $(SUB_DIRECTORIES); do \
	  	(cd $$d; $(MAKE)); \
	done

test: compile
	 $(ERL) $(ERLFLAGS) -s client_tests ping -s client_tests test -s init stop

clean:
	@for d in $(SUB_DIRECTORIES); do \
	  	(cd $$d; $(MAKE) clean); \
	done
	-rm -f erl_crash.dump
	-rm -f *.beam
	-rm -f *.avi
	-rm -f zerot