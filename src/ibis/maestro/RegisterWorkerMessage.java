package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message telling a master that we would like to receive jobs.
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
     * Constructs a new work request message.
     * @param worker Who is asking for work? (-1 means, no ID yet).
     * @param port The receive port to use to submit jobs.
     * @param identifier The identifier to use.
     * @param allowedTypes Which types of jobs can it handle?
     */
    RegisterWorkerMessage( ReceivePortIdentifier port, int identifier, JobType allowedType ){
        // FIXME: move source down from superclasses.
	super( -1 );
	this.port = port;
	this.masterIdentifier = identifier;
    }
}
