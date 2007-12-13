package ibis.maestro;

/**
 * The interface of a job in the Maestro master/worker system.
 * @author Kees van Reeuwijk
 *
 */
public interface Job extends Comparable<Job> {
    /**
     * Runs the job.
     * @return The result of the job.
     */
    JobResult run();
}
