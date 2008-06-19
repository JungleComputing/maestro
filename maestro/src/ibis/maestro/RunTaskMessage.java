package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;


/**
 * Tell the worker to execute the task contained in this message.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class RunTaskMessage extends MasterMessage {
    /** */
    private static final long serialVersionUID = 1L;
    final WorkerIdentifier workerIdentifier;
    final TaskInstance task;
    final long taskId;
    private transient long queueTime = 0;
    private transient long runTime = 0;

    /**
     * Given a task and its source, constructs a new RunTaskMessage.
     * @param source Who sent this task, as an identifier we know about.
     * @param task The task to run.
     * @param taskId The identifier of the task.
     */
    RunTaskMessage( Worker.MasterIdentifier source, WorkerIdentifier workerIdentifier, TaskInstance task, long taskId )
    {
	super( source );
	this.workerIdentifier = workerIdentifier;
	this.task = task;
        this.taskId = taskId;
    }

    /** Set the start time of this task to the given time in ns.
     * @param t The start time.
     */
    void setQueueTime(long t) {
        this.queueTime = t;
    }

    /**
     * Registers the given time as the moment this task started running.
     * @param t The start time.
     */
    void setRunTime(long t )
    {
        this.runTime = t;
    }

    /** Returns the registered enqueueing time.
     * 
     * @return The registered enqueueing time.
     */
    long getQueueTime() {
        return queueTime;
    }

    /** Returns the registered start time.
     * 
     * @return The registered start time.
     */
    long getRunTime() {
        return runTime;
    }

    /**
     * Returns a string representation of this task messabge.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "Task message for task " + taskId + " of type " + task.type;
    }
}
