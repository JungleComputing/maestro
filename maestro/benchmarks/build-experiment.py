#!/usr/bin/env python

import sys
from string import Template
import string
jobsPerProcessor=20000

def usage():
    print "Usage: " + sys.argv[0] + " <prefix> <nodes>"

if len(sys.argv) != 3:
    print "I get " + `(len(sys.argv)-1)` + " arguments, while I need 2"
    usage()
    sys.exit( 1 )

prefix=sys.argv[1]
p=int( sys.argv[2] )
if p<1:
    print "Illegal node count " + p + " (argv[2]='" + sys.argv[2] + ")"
    usage()
    sys.exit( 1 )
    


fnm = prefix+"%d.experiment" %p
list = []
for n in range(p):
    list.append( prefix + "-run%d-out.%d" % ( p, n ) )
s = Template( """# Generated experiment file
run$p.application.name = TestProg
run$p.process.count = $p
run$p.cluster.name = VU
run$p.pool.name = $prefix-run$p
run$p.application.output.files = $l
run$p.application.arguments = $n
run$p.resource.count = $p
""" )
print s.substitute( n=p*jobsPerProcessor, prefix=prefix, p=p, l=string.join( list, "," ) )

