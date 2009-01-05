#!/usr/bin/env python

import sys
import string
import constants

workerString = 'Worker:'

def startsWith( s, pre ):
    return s[0:len(pre)] == pre
    
def extractTaskLabel( s ):
    s = s[len(workerString):].strip()
    s = s[5:-2]
    return int( s )

def extractTaskCount( s ):
    s = s.strip()
    el = s.split()
    return int( el[3] )

def getTaskCounts( fnm ):
    fh = open( fnm )
    res = [0]*5
    taskLabel = None
    for line in fh.readlines():
        if taskLabel != None:
            count = extractTaskCount( line )
            res[taskLabel] = count
        if startsWith( line, workerString ) and line[-2] == ':':
            taskLabel = extractTaskLabel( line )
        else:
            taskLabel = None
    fh.close()
    return res

def second_cmp( a, b ):
    return cmp( a[1], b[1] )

def usage():
    print "Usage: " + sys.argv[0] + " <tag> <output-file> <file>...<file>"

if len(sys.argv) < 4:
    print "I get " + `(len(sys.argv)-1)` + " arguments, while I need at least 3"
    usage()
    sys.exit( 1 )

elements = string.split( sys.argv[1], '-' )
sum = 0
count = 0
files = sys.argv[2:]
l = []
for fnm in files:
    counts = getTaskCounts( fnm )
    l.append( counts )

l = sorted( l, second_cmp )
output_file = sys.argv[1]
fhnd = open( output_file, 'w' )
i = 0
for e in l:
    fhnd.write( `i` + ' ' )
    i += 1
    for v in e:
       fhnd.write( `v` + ' ' )
    fhnd.write( '\n' )
fhnd.close()
