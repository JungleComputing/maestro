package ibis.maestro;


/**
 * A message to tell the master not to send jobs to this worker any more.
 * 
 * @author Kees van Reeuwijk.
 */
class WorkerResignMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    WorkerResignMessage( int worker ){
	super( worker );
    }
}
