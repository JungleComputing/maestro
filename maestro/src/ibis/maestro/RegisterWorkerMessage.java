package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

/**
 * A message from a worker to a master, telling it that the worker exists, and which identifier the
 * worker wants the master to use when it talks to it.
 *
 * @author Kees van Reeuwijk
 *
 */
final class RegisterWorkerMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** Our receive port. */
    final ReceivePortIdentifier port;

    final TaskType[] supportedTypes;

    /** Our identifier for the master. */
    final MasterIdentifier masterIdentifier;

    /**
     * Constructs a new worker registration message.
     * @param port The receive port to use to submit tasks.
     * @param masterID The identifier to use.
     */
    RegisterWorkerMessage( ReceivePortIdentifier port, MasterIdentifier masterID, TaskType[] taskTypes )
    {
	super( null );
	this.port = port;
	this.masterIdentifier = masterID;
	this.supportedTypes = taskTypes;
    }

    /**
     * Returns a string representation of this message.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        String tl = "";
        
        for( TaskType t: supportedTypes ) {
            if( !tl.isEmpty() ) {
                tl += ',';
            }
            tl += t;
        }
        return "register worker " + port + "; supported types: [" + tl + ']';
    }
}
