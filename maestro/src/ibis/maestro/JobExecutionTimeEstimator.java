package ibis.maestro;

/**
 * Tasks implementing this interface provide an initial estimate of their
 * execution time.
 * 
 * @author Kees van Reeuwijk.
 */
public interface JobExecutionTimeEstimator {
    /**
     * Returns an estimate in nanoseconds of the compute time of this task.
     * 
     * @return The estimated compute time of this task in nanoseconds.
     */
    double estimateJobExecutionTime();
}
