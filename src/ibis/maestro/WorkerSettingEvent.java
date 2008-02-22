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
    final long roundTripTime;

    /**
     * Constructs a new WorkerSettingEvent.
     * @param me The master that does the setting.
     * @param port For which worker the setting is updated.
     * @param roundTripTime  The estimated time in ns to transmit a job an get a result back.
     */
    public WorkerSettingEvent(ReceivePortIdentifier me, ReceivePortIdentifier port, long roundTripTime )
    {
	super( System.nanoTime() );
	this.master = me;
	this.worker = port;
        this.roundTripTime = roundTripTime;
    }

    /** Returns a string containing information about this event.
     * 
     * @return The information string.
     */
    public String getInfo()
    {
        return "set worker " + worker + " roundTripTime=" + Service.formatNanoseconds( roundTripTime );
    }
    
}
