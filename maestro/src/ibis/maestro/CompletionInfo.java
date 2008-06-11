package ibis.maestro;

/**
 * A class representing the completion interval for the given type.
 *
 * @author Kees van Reeuwijk
 */
class CompletionInfo {
    final JobType type;
    final long completionInterval;

    // TODO: instead of storing the info per type, use an array for a task
    // and just mark the unknown values.
    /**
     * @param type The type of job.
     * @param completionInterval The completion interval.
     */
    CompletionInfo(JobType type, long completionInterval) {
	this.type = type;
	this.completionInterval = completionInterval;
    }

}
