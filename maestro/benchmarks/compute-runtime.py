#!/usr/bin/env python

import sys
import string

def usage():
    print "Usage: " + sys.argv[0] + " <specification>"

if len(sys.argv) != 2:
    print "I get " + `(len(sys.argv)-1)` + " arguments, while I need 1"
    usage()
    sys.exit( 1 )

arg = sys.argv[1]
elements = arg.split( '-' )
p=int( elements[1] )
if p<1:
    print "Illegal node count " + p + " (argv[1]='" + sys.argv[1] + ")"
    usage()
    sys.exit( 1 )

jobsPerProcessor=int( elements[2] )
if jobsPerProcessor<2:
    print "Illegal number of jobs per processor " + p + " (argv[2]='" + sys.argv[2] + ")"
    usage()
    sys.exit( 1 )

fiveMinutes = 5*60

runtime = 2*jobsPerProcessor/3
if runtime<fiveMinutes:
    runtime = fiveMinutes
print "RUNTIME=%d" % runtime
