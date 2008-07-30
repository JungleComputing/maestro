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
    final long completionInterval;

    /**
     * @param type The type of task.
     * @param workerDwellTime The time this task will dwell on this worker for queueing and computation.
     * @param completionInterval The completion interval.
     */
    CompletionInfo( TaskType type, long completionInterval )
    {
        this.type = type;
        this.completionInterval = completionInterval;
    }

    /**
     * Returns a string representation of this completion info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "completionInterval" + type + "=" + Service.formatNanoseconds( completionInterval );
    }
}
