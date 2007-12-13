package ibis.maestro;

/**
 * The interface of Maestro job completion listeners.
 * 
 * @author Kees van Reeuwijk
 *
 */
public interface CompletionListener {
    /**
     * Registers that a job is completed.
     * @param j The Maestro job that was completed.
     */
    void jobCompleted( Job j );
}
