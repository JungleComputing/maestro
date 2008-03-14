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
     * @param context The context of this job submission.
     */
    void run( Node node );

    /**
     * Returns the type of this job.
     * Different job types get different scheduling statistics.
     * @return The type of this job.
     */
    JobType getType();
}
