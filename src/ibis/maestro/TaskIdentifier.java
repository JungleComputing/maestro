package ibis.maestro;

import java.io.Serializable;

/**
 * FIXME.
 *
 * @author Kees van Reeuwijk.
 */
public interface TaskIdentifier extends Serializable
{
    /** Reports the result of the task back to the original submitter.
     * 
     * @param node The node we're running on.
     * @param result The result to report.
     */
    void reportResult( Node node, JobResultValue result );
}
