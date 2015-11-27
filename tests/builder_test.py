from nose.tools import *
import os.path

import bs

def root_test():
    builder = bs.Bs()
    eq_(str(builder.root.resolve()), os.path.abspath(os.path.dirname(__file__)))
