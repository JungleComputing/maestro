package ibis.maestro;

import java.io.Serializable;


/**
 * The interface of a Maestro job completion listener.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface JobCompletionListener {
    /**
     * Registers that a job has completed, and handles the final result of the
     * job.
     * 
     * @param node
     *            The node we're running on.
     * @param id
     *            The identifier of the job that was completed. This identifier
     *            was provided by the user program at the moment the job
     *            instance was submitted.
     * @param result
     *            The result of the job.
     */
    void jobCompleted(Node node, Object id, Serializable result);
}
