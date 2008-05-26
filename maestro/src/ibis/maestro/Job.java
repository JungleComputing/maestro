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

     * @param input The input value of this job run.
     * @param node The node this job is running on.
     * @param context The user-supplied context on this node.
     * @return The result of the job run.
     */
    Object run( Object input, Node node, Context context );
}
