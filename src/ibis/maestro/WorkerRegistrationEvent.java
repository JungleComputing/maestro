package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A trace event that records the addition of a worker to the list
 * of known workers of a master.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class WorkerRegistrationEvent extends TraceEvent {
    /** */
    private static final long serialVersionUID = -5267884426979036349L;
    final ReceivePortIdentifier master;
    final ReceivePortIdentifier worker;
    final double benchmarkScore;
    final long pingTime;
    final long computeTime;

    /**
     * @param me The master that registers the worker.
     * @param port The receive port of the worker.
     * @param benchmarkScore The benchmark score of the worker.
     * @param pingTime The time it took to ping the worker.
     * @param computeTime The time it took the worker to compute its benchmark.
     */
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
