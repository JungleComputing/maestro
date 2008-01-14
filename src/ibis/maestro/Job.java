package ibis.maestro;

import java.io.Serializable;

/**
 * The interface of a job in the Maestro master/worker system.
 * @author Kees van Reeuwijk
 *
 */
public interface Job extends Comparable<Job>, Serializable {
    /**
     * Runs the job.
     * @return The result of the job.
     */
    JobReturn run();
}
