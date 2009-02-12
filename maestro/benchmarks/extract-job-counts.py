#!/usr/bin/env python

import sys
import math
import string
import constants

workerString = 'Worker:'

def startsWith( s, pre ):
    return s[0:len(pre)] == pre
    

def computeVariation( l ):
    sum = 0
    for e in l:
        sum += e
    average = float( sum )/len( l )
    diff = 0
    for e in l:
        diff += math.fabs( float( e )-average )
    return diff

def extractJobLabel( s ):
    s = s[len(workerString):].strip()
    s = s[5:-2]
    return int( s )

def extractJobCount( s ):
    s = s.strip()
    el = s.split()
    return int( el[3] )

def getJobCounts( fnm ):
    fh = open( fnm )
    res = [0]*5
    jobLabel = None
    for line in fh.readlines():
        if jobLabel != None:
            count = extractJobCount( line )
            res[jobLabel] = count
        if startsWith( line, workerString ) and line[-2] == ':':
            jobLabel = extractJobLabel( line )
        else:
            jobLabel = None
    fh.close()
    return res

def variation_cmp( a, b ):
    res = cmp( a[1]/500, b[1]/500 )
    if( res != 0 ):
        return res
    return cmp( computeVariation( a ), computeVariation( b ) )

def usage():
    print "Usage: " + sys.argv[0] + " <tag> <output-file> <file>...<file>"

if len(sys.argv) < 4:
    print "I get " + `(len(sys.argv)-1)` + " arguments, while I need at least 3"
    usage()
    sys.exit( 1 )

elements = string.split( sys.argv[1], '-' )
sum = 0
count = 0
files = sys.argv[3:]
l = []
for fnm in files:
    counts = getJobCounts( fnm )
    l.append( counts )

l = sorted( l, variation_cmp )
output_file = sys.argv[2]
fhnd = open( output_file, 'w' )
i = 0
for e in l:
    fhnd.write( `i` + ' ' )
    i += 1
    for v in e:
       fhnd.write( `v` + ' ' )
    fhnd.write( '\n' )
fhnd.close()
