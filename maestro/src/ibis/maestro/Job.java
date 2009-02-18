package ibis.maestro;


/**
 * The super-interface of all variations of jobs.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface Job {
    /**
     * Returns true iff this job can run on this node.
     * 
     * @return True iff this job can run.
     */
    abstract boolean isSupported();
}
