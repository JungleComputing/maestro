package ibis.maestro;

/**
 * The interface of a task for which the performance is unpredictable. Such a
 * task will only be submitted to nodes that have an idle processor.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface UnpredictableAtomicTask extends AtomicTask,
	TaskExecutionTimeEstimator {
    // Just a marker interface.
}
