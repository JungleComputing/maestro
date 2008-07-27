package ibis.maestro;



/**
 * Tell the worker to execute the task contained in this message.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class RunTaskMessage extends Message
{
    /** */
    private static final long serialVersionUID = 1L;
    final NodeIdentifier workerIdentifier;
    final TaskInstance taskInstance;
    final long taskId;
    private transient long queueMoment = 0L;
    private transient int queueLength = 0;
    final NodeIdentifier source;

    /**
     * Given a task and its source, constructs a new RunTaskMessage.
     * @param source Who sent this task, as an identifier we know about.
     * @param task The task to run.
     * @param taskId The identifier of the task.
     */
    RunTaskMessage( NodeIdentifier source, NodeIdentifier workerIdentifier, TaskInstance task, long taskId )
    {
        this.source = source;
	this.workerIdentifier = workerIdentifier;
	this.taskInstance = task;
        this.taskId = taskId;
    }

    /** Set the start time of this task to the given time in ns.
     * @param t The start time.
     * @param queueLength The length of the work queue at this moment.
     */
    void setQueueMoment( long t, int queueLength )
    {
        this.queueMoment = t;
        this.queueLength = queueLength;
    }

    /** Returns the registered enqueuing moment.
     * 
     * @return The registered enqueuing moment.
     */
    long getQueueMoment()
    {
        return queueMoment;
    }

    /** Returns the registered queue length at the moment of enqueuing.
     * 
     * @return The registered queue length.
     */
    int getQueueLength()
    {
        return queueLength;
    }

    /**
     * Returns a string representation of this task messabge.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "Task message for task " + taskId + " of type " + taskInstance.type;
    }
}
