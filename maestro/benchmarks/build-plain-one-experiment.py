#!/usr/bin/env python

import sys
from string import Template
import string
jobsPerProcessor=1000

def usage():
    print "Usage: " + sys.argv[0] + " <nodes>"

if len(sys.argv) != 2:
    print "I get " + `(len(sys.argv)-1)` + " arguments, while I need 1"
    usage()
    sys.exit( 1 )

p=int( sys.argv[1] )
if p<1:
    print "Illegal node count " + p + " (argv[1]='" + sys.argv[1] + ")"
    usage()
    sys.exit( 1 )
    
list = []
for n in range(p):
    list.append( "plain-run%d-out.%d" % ( p, n ) )
    args = "-onetask,%d" % (p*jobsPerProcessor)
s = Template( """# Generated experiment file
run$p.application.name = VideoPlayerBenchmarkProgram
run$p.process.count = $p
run$p.cluster.name = VU
run$p.pool.name = plain-one-run$p
run$p.application.output.files = $l
run$p.application.arguments = $args
run$p.resource.count = $p
""" )
print s.substitute( args=args, p=p, l=string.join( list, "," ) )

