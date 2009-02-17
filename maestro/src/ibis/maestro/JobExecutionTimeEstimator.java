package ibis.maestro;

/**
 * Jobs implementing this interface provide an initial estimate of their
 * execution time.
 * 
 * @author Kees van Reeuwijk.
 */
public interface JobExecutionTimeEstimator {
    /**
     * Returns an estimate in nanoseconds of the compute time of this job.
     * 
     * @return The estimated compute time of this job in nanoseconds.
     */
    double estimateJobExecutionTime();
}
