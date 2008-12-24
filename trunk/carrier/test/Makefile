ERL = erl
ERLFLAGS = -noshell -sname llk -setcookie abc

.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W $<

.yrl.erl:
	erlc -W $<

all: compile test

compile: client.beam t_client.beam

client.beam:../client/client.erl
	erlc ../client/client.erl

test: compile
	 $(ERL) $(ERLFLAGS) -s t_client ping -s t_client test client.beam -s init stop

clean:
	-rm -f erl_crash.dump
	-rm -f *.beam
	-rm -f *.avi
	-rm -f zerot