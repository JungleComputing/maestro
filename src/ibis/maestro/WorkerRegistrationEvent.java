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

    /**
     * @param me The master that registers the worker.
     * @param port The receive port of the worker.
     */
    public WorkerRegistrationEvent(ReceivePortIdentifier me, ReceivePortIdentifier port )
    {
	super( System.nanoTime() );
	this.master = me;
	this.worker = port;
    }

}
