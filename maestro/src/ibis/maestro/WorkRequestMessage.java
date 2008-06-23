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

    /** For each type of task we know, the estimated time it will
     * take to complete the remaining tasks of this job.
     */
    final CompletionInfo[] completionInfo;

    /** For each type of task we know, the worker queue length.     */
    final WorkerQueueInfo[] workerQueueInfo;

    /**
     * Constructs a new work request message.
     * @param identifier The identifier to use.
     * @param workerQueueInfo 
     */
    WorkRequestMessage( WorkerIdentifier identifier, CompletionInfo[] completionInfo, WorkerQueueInfo[] workerQueueInfo ){
	super( identifier );
	this.completionInfo = completionInfo;
	this.workerQueueInfo = workerQueueInfo;
    }
}
