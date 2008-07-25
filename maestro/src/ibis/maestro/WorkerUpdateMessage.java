package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;

/**
 * A message from a worker to a master, telling it about its current
 * job completion times.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class WorkerUpdateMessage extends Message {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** For each type of task we know, the estimated time it will
     * take to complete the remaining tasks of this job.
     */
    final CompletionInfo[] completionInfo;

    /** For each type of task we know, the queue length on this worker. */
    final WorkerQueueInfo[] workerQueueInfo;

    final Master.WorkerIdentifier source;

    /**
     * Constructs a new work request message.
     * @param identifier The identifier to use.
     */
    WorkerUpdateMessage( WorkerIdentifier identifier, CompletionInfo[] completionInfo, WorkerQueueInfo[] workerQueueInfo )
    {
	source = identifier;
	this.completionInfo = completionInfo;
	this.workerQueueInfo = workerQueueInfo;
    }
}
