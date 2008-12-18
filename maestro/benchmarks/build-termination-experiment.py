#!/usr/bin/env python

import sys
from string import Template
import string
import constants
jobsPerProcessor=constants.jobsPerProcessor
learnJobsPerProcessor=constants.learnJobsPerProcessor

def usage():
    print "Usage: " + sys.argv[0] + " <specification>"

if len(sys.argv) != 2:
    print "I get " + `(len(sys.argv)-1)` + " arguments, while I need 1"
    usage()
    sys.exit( 1 )

arg = sys.argv[1]
elements = arg.split( '-' )
frac=elements[2]
p=constants.processorsForFaultTolerance

jobs = `p*jobsPerProcessor`
# Translation table from measurement type to command-line arguments
args = {
    'plain':[jobs],
    'slow':['-oddslowscale','-evenslowsharpen',jobs],
}

label=sys.argv[1]
label = label.replace( '.', '-' )

props='ibis.maestro.terminatorStartQuotum=0,ibis.maestro.terminatorInitialSleepTime=1000,ibis.maestro.terminatorSleepTime=100,ibis.maestro.terminatorNodeQuotum=' + frac

if not elements[1] in args.keys():
    l = string.join( args.keys(), ',' )
    print "Unknown benchmark type '" + elements[1] + "'; I only know [" + l + "]"
    sys.exit( 1 )
arguments = args[elements[1]]
s = Template( """# Generated experiment file
$label.application.name = VideoPlayerBenchmarkProgram
$label.process.count = $p
$label.cluster.name = VU
$label.pool.name = $arg-pool
$label.application.input.files = settag-$arg.sh
$label.application.output.files = $arg.logs
$label.application.arguments = $args
$label.application.system.properties = $props
$label.resource.count = $p
""" )
print s.substitute( label=label, props=props, arg=arg, args=string.join( arguments, ',' ), p=p )

