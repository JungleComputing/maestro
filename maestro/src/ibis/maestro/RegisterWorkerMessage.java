package ibis.maestro;

import java.util.ArrayList;

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

    final JobType[] supportedTypes;

    /** Our identifier for the master. */
    final MasterIdentifier masterIdentifier;

    /**
     * Constructs a new worker registration message.
     * @param port The receive port to use to submit jobs.
     * @param masterID The identifier to use.
     */
    RegisterWorkerMessage( ReceivePortIdentifier port, MasterIdentifier masterID, JobType[] jobTypes )
    {
	super( null );
	this.port = port;
	this.masterIdentifier = masterID;
	this.supportedTypes = jobTypes;
    }

    @Override
    public String toString()
    {
        String tl = "";
        
        for( JobType t: supportedTypes ) {
            if( !tl.isEmpty() ) {
                tl += ',';
            }
            tl += t;
        }
        return "register worker " + port + "; supported types: [" + tl + ']';
    }
}
