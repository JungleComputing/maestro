package ibis.maestro;

import java.io.Serializable;

/**
 * The interface of a job in the Maestro master/worker system.
 * @author Kees van Reeuwijk
 *
 */
public interface Job extends Serializable {
    /**
     * Runs the job.

     * @param input The input value of this job run.
     * @param node The node this job runs on.
     * @param taskId The identifier of the task this job belongs to.
     * @return The result of the job run.
     */
    Object run( Object input, Node node, TaskInstanceIdentifier taskId );
}
