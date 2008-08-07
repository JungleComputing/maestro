package ibis.maestro;

/**
 * The interface of a task for which the performance is unpredictable.
 * Such a task will only be placed on nodes that have an idle processor.
 * 
 * @author Kees van Reeuwijk
 *
 */
public interface UnpredictableTask extends Task, TaskExecutionTimeEstimator
{
}
