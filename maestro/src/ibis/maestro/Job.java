package ibis.maestro;

/**
 * The super-interface of all variations of tasks.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface Job {
    /**
     * Returns true iff this task can run in this context.
     * 
     * @return True iff this task can run.
     */
    abstract boolean isSupported();
}
