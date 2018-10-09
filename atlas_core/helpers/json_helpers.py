"""This module is specifically not named 'json' so as to not collide with the
python standard library json."""

import json
from collections import OrderedDict


def add_preamble(file_name, key="data"):
    with open(file_name, "r+") as f:
        network = json.loads(f.read(), object_pairs_hook=OrderedDict)
        f.seek(0)
        f.truncate()
        f.write(json.dumps({key: network}, indent=4, separators=(",", ": ")))


def strip_preamble(file_name, key="data"):
    with open(file_name, "r+") as f:
        data = json.loads(f.read(), object_pairs_hook=OrderedDict)
        f.seek(0)
        f.truncate()
        f.write(json.dumps(data[key], indent=4, separators=(",", ": ")))


def json_read(file_name):
    """Read a json file while maintaining record order."""
    with open(file_name, "r+") as f:
        return json.loads(f.read(), object_pairs_hook=OrderedDict)
