package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;

/**
 * A message from a worker to a master, telling it that the worker would like
 * to receive more work.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class WorkRequestMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** For each type of job we know, the estimated time it will
     * take to complete the remaining jobs of this task.
     */
    final CompletionInfo[] completionInfo;

    /**
     * Constructs a new work request message.
     * @param identifier The identifier to use.
     */
    WorkRequestMessage( WorkerIdentifier identifier, CompletionInfo[] completionInfo ){
	super( identifier );
	this.completionInfo = completionInfo;
    }
}
