package ibis.maestro;

import java.io.Serializable;

/**
 * A class representing the current worker queue length for the given type.
 *
 * @author Kees van Reeuwijk
 */
class WorkerQueueInfo implements Serializable
{
    private static final long serialVersionUID = 1L;
    TaskType type;
    final int queueLength;
    final long dequeueTime;
    final long computeTime;

    // TODO: instead of storing the info per type, use an array for a job
    // and just mark the unknown values.
    /**
     * @param type The type of task.
     * @param queueLength The worker queue length.
     */
    WorkerQueueInfo( TaskType type, int queueLength, long dequeueTime, long computeTime )
    {
	this.type = type;
	this.queueLength = queueLength;
	this.dequeueTime = dequeueTime;
	this.computeTime = computeTime;
    }

    /**
     * Returns a string representation of this completion info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "WorkerQueueInfo: type " + type + ": queueLength=" + queueLength + " dequeueTime=" + Service.formatNanoseconds( dequeueTime ) + " computeTime=" + Service.formatNanoseconds( computeTime );
    }
}
