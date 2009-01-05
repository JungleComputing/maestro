#!/usr/bin/env python

import sys
import string

def usage():
    print "Usage: " + sys.argv[0] + " <specification>"

if len(sys.argv) < 5:
    print "I get " + `(len(sys.argv)-1)` + " arguments, while I need at least 5"
    usage()
    sys.exit( 1 )

base = float( sys.argv[2] )

for f in sys.argv[4:]:
    frac = float( f )
    t = base/(1-frac)
    print "%s %f" %(f, t)
