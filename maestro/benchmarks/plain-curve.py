#!/usr/bin/env python

from string import Template
import string
prefix="plain-run"

points = [1,2,4,8,16]

for p in points:
    fnm = prefix+"%d.experiment" %p
    list = []
    for n in range(p):
        list.append( "run%d-out%d" % ( p, n ) )
    s = Template( """# Generated experiment file
run$p.application.name = TestProg
run$p.process.count = $p
run$p.cluster.name = VU
run$p.pool.name = plain-run-$p
default.output.files = $l
run$p.resource.count = $p
""" )
    print "fnm=" + fnm
    f = open( fnm, 'w' )
    f.write( s.substitute( p=p, l=string.join( list, "," ) ) )
    f.close

