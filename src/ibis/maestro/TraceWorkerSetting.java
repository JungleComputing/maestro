package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

public class TraceWorkerSetting extends TraceEvent {
    final ReceivePortIdentifier master;
    final ReceivePortIdentifier worker;
    final int workThreads;
    final double benchmarkScore;
    final long roundTripTime;
    final long computeTime;
    final long preCompletionInterval;

    public TraceWorkerSetting(ReceivePortIdentifier me, ReceivePortIdentifier port, int workThreads, double benchmarkScore, long roundTripTime, long computeTime, long preCompletionInterval)
    {
	super( System.nanoTime() );
	this.master = me;
	this.worker = port;
	this.workThreads = workThreads;
	this.benchmarkScore = benchmarkScore;
	this.roundTripTime = roundTripTime;
	this.computeTime = computeTime;
	this.preCompletionInterval = preCompletionInterval;
    }

}
