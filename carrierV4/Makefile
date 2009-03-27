# leave these lines alone
.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W $<

all: compile

compile: ${MODS:%=%.beam} subdirs
	

## run an application from the makefile

ms:compile
	cd metaserver; make ms 

ds:compile
	cd dataserver; make ds 

cl1:compile
	cd client; make cl1 
cl2:compile
	cd client; make cl2 
cl3:compile
	cd client; make cl3 

#gnibre use in MinWG
wms:compile
	cd metaserver; make wms &

wds:compile
	cd dataserver; make wds &

wcl:compile
	cd client;	make wcl &


# the subdirs target compiles any code in 
# sub-directories

subdirs:
	cd lib;			make
	#cd metaserver; 	make
	cd dataserver; 	make
	cd client; 		make

# remove all the code

clean:	
	rm -rf *.beam erl_crash.dump
	cd lib;			make clean
	#cd metaserver; 	make clean
	cd dataserver; 	make clean
	cd client; 		make clean	

