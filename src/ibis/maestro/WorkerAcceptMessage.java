
package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message from a master to a worker, telling it that it has been accepted and what the receive port of the master is.
 *
 * @author Kees van Reeuwijk.
 */
public class WorkerAcceptMessage extends MasterMessage
{
    /** Contractual obligation. */
    private static final long serialVersionUID = 141652L;
    final ReceivePortIdentifier port;
    final int identifierOnMaster;

    /**
     * Given some essential information, constructs a new WorkerAcceptMessage.
     * @param identifierOnWorker The identifier the worker uses for this master.
     * @param port The receive port of the master.
     * @param identifierOnMaster The identifier the master uses for this worker.
     */
    public WorkerAcceptMessage( int identifierOnWorker, ReceivePortIdentifier port, int identifierOnMaster )
    {
        super( identifierOnWorker );
        this.port = port;
        this.identifierOnMaster = identifierOnMaster;
    }
    
    /**
     * Returns a string representation of this message. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "Accept worker " + source + " port=" + port;
    }

}
