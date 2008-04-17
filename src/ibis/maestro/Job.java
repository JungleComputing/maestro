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
    void run( Node node, TaskInstanceIdentifier taskId );

    /**
     * Returns the type of this job.
     * Different job types are treated differently during scheduling.
     * @return The type of this job.
     */
    JobType getType();
}
