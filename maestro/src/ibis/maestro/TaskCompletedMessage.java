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

    /** The time interval this task spent in the worker queue. */
    final long queueInterval;
    
    /** The time interval this task required to actually compute. */
    final long computeInterval;

    /** For each type of task we know, the estimated time it will
     * take to complete the remaining tasks of this job.
     */
    final CompletionInfo[] completionInfo;

    /**
     * Constructs a task-completed message for the master of a task.
     * @param src The worker that handled the task.
     * @param taskId The identifier of the task, as handed out by the master.
     * @param queueInterval The time interval the task spent in the worker queue.
     * @param computeInterval The time interval that was spent to compute the task.
     * @param completionInfo The estimated time it will take on this
     *     worker to complete the remaining tasks of this job.
     */
    TaskCompletedMessage( Master.WorkerIdentifier src, long taskId, long queueInterval, long computeInterval, CompletionInfo[] completionInfo )
    {
	super( src );
        this.taskId = taskId;
        this.queueInterval = queueInterval;
        this.computeInterval = computeInterval;
        this.completionInfo = completionInfo;
    }

    /**
     * Returns a string representation of this status message.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "worker status message: taskId=" + taskId + " queueInterval=" + Service.formatNanoseconds(queueInterval);
    }
}
