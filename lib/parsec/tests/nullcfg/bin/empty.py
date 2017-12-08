#!/usr/bin/env python
"""
An empty config file should successfully yield an empty sparse config dict.
"""


import os, sys

fpath = os.path.dirname(os.path.abspath(__file__))
# parsec
sys.path.append(fpath + '/../../..')


from parsec.config import config
from parsec.validate import validator as vdr
from parsec.OrderedDict import OrderedDict

SPEC = {
    'meta': {
        'title': vdr(vtype="string")
        }
    }
cfg = config(SPEC)
cfg.loadcfg("empty.rc")

if cfg.get(sparse=True) != OrderedDict():
    sys.exit(1)
