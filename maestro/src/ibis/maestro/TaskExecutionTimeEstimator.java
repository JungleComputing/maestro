package ibis.maestro;

/**
 * Tasks implementing this interface can provide an initial estimate of their execution time
 *
 * @author Kees van Reeuwijk.
 */
public interface TaskExecutionTimeEstimator
{
    /**
     * Returns an estimate of the compute time of this task.
     * @return The estimated compute time of this task in nanoseconds.
     */
    long estimateTaskExecutionTime();
}
