package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message from a worker to a master, telling it that the worker exists, and which identifier the
 * worker wants the master to use when it talks to it.
 *
 * @author Kees van Reeuwijk
 *
 */
public class RegisterWorkerMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** Our receive port. */
    final ReceivePortIdentifier port;

    /** Our identifier for the master. */
    final int masterIdentifier;

    /**
     * Constructs a new worker registration message.
     * @param port The receive port to use to submit jobs.
     * @param identifier The identifier to use.
     */
    RegisterWorkerMessage( ReceivePortIdentifier port, int identifier )
    {
	super( -1 );
	this.port = port;
	this.masterIdentifier = identifier;
    }
}
