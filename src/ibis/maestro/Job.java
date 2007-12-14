package ibis.maestro;

/**
 * The interface of a job in the Maestro master/worker system.
 * @author Kees van Reeuwijk

 * @param <T> The type of the result of the job.
 */
public interface Job<T> extends Comparable<Job<T>> {
    /**
     * Runs the job.
     * @return The result of the job.
     */
    T run();
}
