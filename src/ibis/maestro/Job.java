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
     *
     * @param node The node this job runs on.
     * @param taskId The identifier of the task this job belongs to.
     */
    Object run( Object input, Node node, TaskInstanceIdentifier taskId );
}
