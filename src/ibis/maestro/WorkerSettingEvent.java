package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * An entry that is registered on the event of updating worker settings in a master. 
 *
 * @author Kees van Reeuwijk.
 */
public class WorkerSettingEvent extends TraceEvent {
    final ReceivePortIdentifier master;
    final ReceivePortIdentifier worker;
    final double benchmarkScore;
    final long pingTime;
    final long computeTime;

    /**
     * Constructs a new WorkerSettingEvent.
     * @param me The master that does the setting.
     * @param port For which worker the setting is updated.
     * @param benchmarkScore
     * @param pingTime
     * @param computeTime
     */
    public WorkerSettingEvent(ReceivePortIdentifier me, ReceivePortIdentifier port, double benchmarkScore, long pingTime, long computeTime)
    {
	super( System.nanoTime() );
	this.master = me;
	this.worker = port;
	this.benchmarkScore = benchmarkScore;
        this.pingTime = pingTime;
	this.computeTime = computeTime;
    }

    
}
