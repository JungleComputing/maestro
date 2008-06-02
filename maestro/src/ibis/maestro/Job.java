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
     * @param context The program context.
     * @return The result of the job run.
     */
    Object run( Object input, Node node, Context context );

    /**
     * Returns true iff this job can run in this context.
     * @param context The program context.
     * @return True iff this job can run.
     */
    abstract boolean isSupported( Context context );
}
