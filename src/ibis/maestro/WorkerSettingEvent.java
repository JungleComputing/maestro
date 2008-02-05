package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * An entry that is registered on the event of updating worker settings in a master. 
 *
 * @author Kees van Reeuwijk.
 */
public class WorkerSettingEvent extends TraceEvent {
    /** */
    private static final long serialVersionUID = -7896488360789423920L;
    final ReceivePortIdentifier master;
    final ReceivePortIdentifier worker;
    final long computeTime;
    final long roundTripTime;
    final long preCompletionInterval;

    /**
     * Constructs a new WorkerSettingEvent.
     * @param me The master that does the setting.
     * @param port For which worker the setting is updated.
     * @param roundTripTime 
     * @param benchmarkScore
     * @param pingTime
     * @param computeTime
     * @param preCompletionInterval 
     */
    public WorkerSettingEvent(ReceivePortIdentifier me, ReceivePortIdentifier port, long roundTripTime, long computeTime, long preCompletionInterval )
    {
	super( System.nanoTime() );
	this.master = me;
	this.worker = port;
	this.preCompletionInterval = preCompletionInterval;
        this.roundTripTime = roundTripTime;
	this.computeTime = computeTime;
    }
    
}
