#
#   MIT Licensed
#
#   git@github.com/rstms/tzc
#   mkrueger@rstms.net
#

PROJECT=$(notdir $(shell pwd))

help:
	@echo Project: $(PROJECT)
	@echo Targets: $$(awk -F: '/^[[:graph:]]*:/{print $$1}' Makefile)

install-test:
	sudo pip install -U -e .[test]

install:
	sudo pip install -U -e .

test: install
	dotenv run pytest

debug:
	dotenv run pytest --pdb

bump-patch:
	bumpversion patch

bump-minor:
	bumpversion minor

bump-major:
	bumpversion major
