package ibis.maestro;

/**
 * The interface of a job for which the performance is unpredictable.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface UnpredictableAtomicJob extends AtomicJob,
        JobExecutionTimeEstimator {
    // Just a marker interface.
}
