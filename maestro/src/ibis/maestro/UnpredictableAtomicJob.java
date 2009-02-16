package ibis.maestro;

/**
 * The interface of a task for which the performance is unpredictable. Such a
 * task will only be submitted to nodes that have an idle processor.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface UnpredictableAtomicJob extends AtomicJob,
        JobExecutionTimeEstimator {
    // Just a marker interface.
}