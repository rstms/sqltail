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


define bump
bumpversion $1;
dotenv set VERSION $$(cat VERSION);
sed "s/^\(.*__version__.*=.*'\)\(.*\)\('.*\)$$/\1$$(cat VERSION)\3/" -i $(PROJECT)/__init__.py
endef

bump-patch:
	$(call bump,patch)

bump-minor:
	$(call bump,minor)

bump-major:
	$(call bump,major)
