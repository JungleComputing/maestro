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
    final long submissionInterval;
    final long queueInterval;
    final long queueEmptyInterval;

    /**
     * Constructs a new WorkerSettingEvent.
     * @param me The master that does the setting.
     * @param port For which worker the setting is updated.
     * @param roundTripTime  The estimated time in ns to transmit a job an get a result back.
     * @param computeTime The estimated time in ns that is required to compute a job.
     * @param submissionInterval The estimated interval between job submissions.
     * @param queueInterval The time in ns the most recent job had to spend in the worker queue.
     * @param queueEmptyInterval The time in ns the worker queue was idle before the most recent job entered its queue
     */
    public WorkerSettingEvent(ReceivePortIdentifier me, ReceivePortIdentifier port, long roundTripTime, long computeTime, long submissionInterval, long queueInterval, long queueEmptyInterval )
    {
	super( System.nanoTime() );
	this.master = me;
	this.worker = port;
	this.submissionInterval = submissionInterval;
        this.roundTripTime = roundTripTime;
	this.computeTime = computeTime;
        this.queueInterval = queueInterval;
        this.queueEmptyInterval = queueEmptyInterval;
    }

    /** Returns a string containing information about this event.
     * 
     * @return The information string.
     */
    public String getInfo()
    {
        return "set worker " + worker + " submissionInterval=" + Service.formatNanoseconds( submissionInterval ) + " computeTime=" + Service.formatNanoseconds( computeTime ) + " roundTripTime=" + Service.formatNanoseconds( roundTripTime ) + " queueInterval=" + Service.formatNanoseconds(queueInterval) + " queueEmptyInterval=" + Service.formatNanoseconds(queueEmptyInterval);
    }
    
}
