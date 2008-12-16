#!/usr/bin/env python

import sys
import string

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
    print "Usage: " + sys.argv[0] + " <nproc> <file>...<file>"

if len(sys.argv) < 3:
    print "I get " + `(len(sys.argv)-1)` + " arguments, while I need at least 2"
    usage()
    sys.exit( 1 )

elements = split( sys.argv[1], '-' )
label=elements[0]
sum = 0
count = 0
for fnm in sys.argv[2:]:
    d = getDuration( fnm )
    if d == None:
        print 'No ' + durationString + ' found in file "' + fnm + '"'
        sys.exit( 1 )
    else:
        sum += d
        count += 1
print label, 1e-9*(sum/count)
