#!/usr/bin/env python

import sys
from string import Template
import string
import constants
jobsPerProcessor=constants.jobsPerProcessor

# Translation table from measurement type to command-line arguments
args = {
    'plain':[],
    'onetask':['-onetask'],
    'no':['-oddnoscale','-evennosharpen'],
    'slow':['-oddslowscale','-evenslowsharpen']
}

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
    
if not elements[0] in args.keys():
    l = string.join( args.keys(), ',' )
    print "Unknown benchmark type '" + elements[0] + "'; I only know [" + l + "]"
    sys.exit( 1 )
arguments = args[elements[0]]
arguments.append( "%d" % (p*jobsPerProcessor) )
s = Template( """# Generated experiment file
run$p.application.name = VideoPlayerBenchmarkProgram
run$p.process.count = $p
run$p.cluster.name = VU
run$p.pool.name = $arg-pool
run$p.application.input.files = settag-$arg.sh
run$p.application.output.files = $arg.logs
run$p.application.arguments = $args
run$p.resource.count = $p
""" )
print s.substitute( arg=arg, args=string.join( arguments, ',' ), p=p )

