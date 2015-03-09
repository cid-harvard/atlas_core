SHELL=/bin/bash
SHELLOPTS=errexit:pipefail

ENVDIR=env
ACTIVATE:=$(ENVDIR)/bin/activate

.PHONY:	clean

count=10


PYTHON_EXECUTABLE=python3
VIRTUALENV_EXECUTABLE=pyvenv


requirements = requirements.txt requirements-dev.txt
virtualenv: $(ACTIVATE)
$(ACTIVATE): $(requirements)
	test -d $(ENVDIR) || $(VIRTUALENV_EXECUTABLE) $(ENVDIR)
	for f in $?; do \
		. $(ACTIVATE); pip install -r $$f; \
	done
	touch $(ACTIVATE)

dev: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="../conf/dev.py" $(PYTHON_EXECUTABLE) runserver.py

test: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="../../conf/dev.py" py.test --cov atlas_core atlas_core/tests.py

shell: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="../../conf/dev.py" $(PYTHON_EXECUTABLE) atlas_core/manage.py shell

dummy: virtualenv
	. $(ACTIVATE); FLASK_CONFIG="../../conf/dev.py" $(PYTHON_EXECUTABLE) atlas_core/manage.py dummy -n $(count)

docs: virtualenv
	git submodule add git://github.com/kennethreitz/kr-sphinx-themes.git doc/_themes
	. $(ACTIVATE); make -C doc/ html
	open doc/_build/html/index.html

clean:
	rm -rf $(ENVDIR)
	rm -rf doc/_build/
