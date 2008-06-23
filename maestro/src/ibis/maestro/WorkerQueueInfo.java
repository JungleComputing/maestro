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

    // TODO: instead of storing the info per type, use an array for a job
    // and just mark the unknown values.
    /**
     * @param type The type of task.
     * @param queueLength The worker queue length.
     */
    WorkerQueueInfo(TaskType type, int queueLength )
    {
	this.type = type;
	this.queueLength = queueLength;
    }

    /**
     * Returns a string representation of this completion info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "queueLength(" + type + ")=" + queueLength;
    }
}
