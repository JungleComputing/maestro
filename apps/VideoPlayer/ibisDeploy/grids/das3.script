#!/bin/bash

export SGE_ROOT=/usr/local/Cluster-Apps/sge

NR_NODES=$1
shift
shift
JAVA=$1
shift
ARGS=$*

echo prun -1 -no-panda $JAVA $NR_NODES $ARGS $NR_NODES
/usr/local/VU/reserve.sge/bin/prun -1 -asocial -t 4:00:00 -no-panda $JAVA $NR_NODES $ARGS
