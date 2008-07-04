package ibis.maestro;

/**
 * A message from the worker to the master, telling the master that the
 * worker has completed the given task.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class TaskCompletedMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    
    /** The identifier of the task. */
    final long taskId;

    /** For each type of task we know, the estimated time it will
     * take to complete the remaining tasks of this job.
     */
    final CompletionInfo[] completionInfo;

    /** For each type of task we know, the worker queue length. */
    final WorkerQueueInfo[] workerQueueInfo;

    /** The time in ns this task spent on the worker, from arrival of work to transmission
     * of this message.
     */
    final long workerDwellTime;
    /**
     * Constructs a task-completed message for the master of a task.
     * @param src The worker that handled the task.
     * @param taskId The identifier of the task, as handed out by the master.
     * @param completionInfo The estimated time it will take on this
     *     worker to complete the remaining tasks of this job.
     * @param workerQueueInfo The queue length for the different types of task.
     */
    TaskCompletedMessage( Master.WorkerIdentifier src, long taskId, long workerDwellTime, CompletionInfo[] completionInfo, WorkerQueueInfo[] workerQueueInfo )
    {
	super( src );
        this.taskId = taskId;
        this.workerDwellTime = workerDwellTime;
        this.completionInfo = completionInfo;
        this.workerQueueInfo = workerQueueInfo;
    }

    /**
     * Returns a string representation of this status message.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "worker status message: taskId=" + taskId;
    }
}
