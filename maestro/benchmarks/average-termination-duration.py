#!/usr/bin/env python

import sys
import string
import constants

durationString = 'DURATION'

def startsWith( s, pre ):
    return s[0:len(pre)] == pre
    
def getDuration( fnm ):
    fh = open( fnm )
    res = None
    for line in fh.readlines():
        if startsWith( line, durationString ):
            res = int( line[len(durationString):] )
    fh.close()
    return res

def usage():
    print "Usage: " + sys.argv[0] + " <tag> <output-file> <file>...<file>"

if len(sys.argv) < 4:
    print "I get " + `(len(sys.argv)-1)` + " arguments, while I need at least 3"
    usage()
    sys.exit( 1 )

elements = string.split( sys.argv[1], '-' )
sum = 0
count = 0
output_file = sys.argv[2]
files = sys.argv[3:]
for fnm in files:
    d = getDuration( fnm )
    if d != None:
        sum += d
        count += 1

if count<1:
    l = string.join( files, "," )
    print "None of the files [" + l + "] contains the string '" + durationString + "'"
    sys.exit( 1 )

killFraction = float( elements[1] )
frames = constants.terminationJobsPerProcessor
duration = 1e-9*(sum/count)
#timePerFrame = (1-killFraction)*(duration/frames)
timePerFrame = (duration/frames)
fhnd = open( output_file, 'w' )
fhnd.write( "%s %f %f\n" % (elements[1], duration, timePerFrame ) )
fhnd.close()
