package ibis.maestro;

/**
 * The interface of a job in the Maestron master/worker system.
 * @author Kees van Reeuwijk
 *
 */
public interface Job extends Comparable<Job> {
    void run();
}
