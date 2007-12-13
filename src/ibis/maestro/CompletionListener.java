package ibis.maestro;

public interface CompletionListener {
    /**
     * Registers that a job is completed.
     * @param j The job that was completed.
     */
    void jobCompleted( Job j );
}
