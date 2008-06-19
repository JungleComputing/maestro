package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;

/**
 * A message from a worker to a master, telling it about its current
 * job completion times.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class WorkerUpdateMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** For each type of task we know, the estimated time it will
     * take to complete the remaining tasks of this job.
     */
    final CompletionInfo[] completionInfo;

    /**
     * Constructs a new work request message.
     * @param identifier The identifier to use.
     */
    WorkerUpdateMessage( WorkerIdentifier identifier, CompletionInfo[] completionInfo ){
	super( identifier );
	this.completionInfo = completionInfo;
    }
}
