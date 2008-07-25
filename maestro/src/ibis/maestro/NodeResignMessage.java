package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;


/**
 * A message to tell the master not to send tasks to this worker any more.
 * 
 * @author Kees van Reeuwijk.
 */
final class NodeResignMessage extends Message {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    final Master.WorkerIdentifier source;

    NodeResignMessage( WorkerIdentifier worker ){
	source = worker;
    }
}
