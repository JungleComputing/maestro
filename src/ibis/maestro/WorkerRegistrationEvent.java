package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

public class WorkerRegistrationEvent extends TraceEvent {
    final ReceivePortIdentifier master;
    final ReceivePortIdentifier worker;
    final double benchmarkScore;
    final long pingTime;
    final long computeTime;

    public WorkerRegistrationEvent(ReceivePortIdentifier me, ReceivePortIdentifier port, double benchmarkScore, long pingTime, long computeTime )
    {
	super( System.nanoTime() );
	this.master = me;
	this.worker = port;
	this.benchmarkScore = benchmarkScore;
	this.pingTime = pingTime;
	this.computeTime = computeTime;
    }

}
