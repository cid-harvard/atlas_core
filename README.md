Atlas-Core
==========

Test Edit!
[![Build Status](https://travis-ci.org/cid-harvard/atlas_core.svg?branch=master)](https://travis-ci.org/cid-harvard/atlas_core)
[![Coverage Status](https://coveralls.io/repos/cid-harvard/atlas_core/badge.svg)](https://coveralls.io/r/cid-harvard/atlas_core)
[![Documentation Status](https://readthedocs.org/projects/atlas-core/badge/?version=latest)](https://readthedocs.org/projects/atlas-core/?badge=latest)

Documentation can be found [here](http://atlas-core.readthedocs.org/en/latest/).

Development
-----------

Run `make dev` to run the dev server. It'll install all the dependencies if it
has to.

<table>
<tr><th>Command</th><th> What it does </th></tr>
<tr><td>`make dev` </td><td> Run the flask test server in debug mode. </td></tr>
<tr><td>`make test` </td><td> run pytest with coverage</td></tr>
<tr><td>`make dummy` </td><td> Generates some dummy data and dumps ids.</td></tr>
<tr><td>`make shell` </td><td> run an ipython shell where you can play around with objects. The variables `app`, `db` and `models` come preloaded.</td></tr>
<tr><td>`make docs` </td><td> Builds pretty docs and pops open a browser window</td></tr>
<tr><td>`make clean` </td><td> Clean up all the generated gunk</td></tr>
</table>
