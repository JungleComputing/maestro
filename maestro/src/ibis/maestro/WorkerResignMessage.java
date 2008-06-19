package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;


/**
 * A message to tell the master not to send tasks to this worker any more.
 * 
 * @author Kees van Reeuwijk.
 */
final class WorkerResignMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    WorkerResignMessage( WorkerIdentifier worker ){
	super( worker );
    }
}
