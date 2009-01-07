ERL = erl
ERLFLAGS = -pa eunit/ebin -noshell -sname llk -setcookie test 

ERLC = erlc 
ERLCFLAGS = -pa eunit/ebin

SUB_DIRECTORIES = eunit
TEST_OBJS := $(shell ls -1 *_tests.erl)

.SUFFIXES: .erl .beam .yrl

.erl.beam:
	$(ERLC) $(ERLCFLAGS) -W $<

.yrl.erl:
	$(ERLC) $(ERLCFLAGS) -W $<

all: subdirs compile test

compile: eunit client.beam clientlib.beam $(TEST_OBJS:%.erl=%.beam)

client.beam: ../client/client.erl
	$(ERLC) $(ERLCFLAGS) ../client/client.erl

clientlib.beam: ../client/clientlib.erl
	$(ERLC) $(ERLCFLAGS) ../client/clientlib.erl


subdirs:
	@for d in $(SUB_DIRECTORIES); do \
	  	(cd $$d; $(MAKE)); \
	done

test: compile
	@for p in $(TEST_OBJS:%.erl=%); do \
		$(ERL) $(ERLFLAGS) -s $$p ping -s $$p test -s init stop ; \
	done

echo:
	echo $(TEST_OBJS)

clean:
	@for d in $(SUB_DIRECTORIES); do \
	  	(cd $$d; $(MAKE) clean); \
	done
	-rm -f erl_crash.dump
	-rm -f *.beam
	-rm -f *.avi
	-rm -f zerot

md5src:
	md5sum ds/* > ds.md5
	sed 's;ds/;re/;' ds.md5 > re.md5

md5check:
	md5sum -c re.md5

md5: md5src md5check

# gen_server:call({global, metagenserver},{debug,clearShadow}).
# gen_server:call({data_server1, pp@pp}, {echo, "msg"}).
