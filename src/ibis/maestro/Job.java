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
     * @param master The master to submit any new jobs to.
     * @return The result of the job.
     */
    JobReturn run( Master master );

    /**
     * Returns the type of this job.
     * Different job types get different scheduling statistics.
     * @return The type of this job.
     */
    JobType getType();
}
