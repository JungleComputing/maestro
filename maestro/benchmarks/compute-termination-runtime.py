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
p=50  # A termination experiment is always with 50 processors

# .. But a fraction gets killed off, resulting in longer process times
killFraction = float( elements[1] )

jobsPerProcessor=int( 2000/(1.0-killFraction) )
if jobsPerProcessor<2:
    print "Illegal number of jobs per processor " + p + " (argv[2]='" + sys.argv[2] + ")"
    usage()
    sys.exit( 1 )

fiveMinutes = 5*60

runtime = int( 0.7*jobsPerProcessor)
if runtime<fiveMinutes:
    runtime = fiveMinutes
print "RUNTIME=%d" % runtime
