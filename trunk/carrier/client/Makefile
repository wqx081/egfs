.SUFFIXES: .erl .beam .yrl

ERL = erl -boot start_clean -setcookie abc 

all: compile

compile: 
	erl -make

client:compile
	${ERL} -sname client -s ping_server ping 

clean:
	rm -rf *.beam erl_crash.dump
	rm -rf test


