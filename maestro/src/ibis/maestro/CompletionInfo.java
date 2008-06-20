package ibis.maestro;

import java.io.Serializable;

/**
 * A class representing the completion interval for the given type.
 *
 * @author Kees van Reeuwijk
 */
class CompletionInfo implements Serializable
{
    private static final long serialVersionUID = 1L;
    final TaskType type;
    final int queueLength;
    final long completionInterval;

    // TODO: instead of storing the info per type, use an array for a job
    // and just mark the unknown values.
    /**
     * @param type The type of task.
     * @param completionInterval The completion interval.
     */
    CompletionInfo(TaskType type, int queueLength, long completionInterval) {
	this.type = type;
	this.queueLength = queueLength;
	this.completionInterval = completionInterval;
    }

    /**
     * Returns a string representation of this completion info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "completionInterval(" + type + ")=" + Service.formatNanoseconds( completionInterval );
    }
}
