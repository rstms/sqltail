#
# sqltail dev/test comands
#

help:
	@echo Targets: run test

run:
	dev make run

test:
	dev make test 

debug:
	dev make debug

install:
	cd src/sqltail; sudo pip install -U . 
