package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;


/**
 * A message to tell the master not to send jobs to this worker any more.
 * 
 * @author Kees van Reeuwijk.
 */
class WorkerResignMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    WorkerResignMessage( WorkerIdentifier worker ){
	super( worker );
    }
}
