package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;

/**
 * The identifier of a task.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskIdentifier implements Serializable
{
    private static final long serialVersionUID = -7567750999837567234L;
    private static long serialNo = 0;

    final long id;
    final Object userId;
    private final ReceivePortIdentifier receivePort;

    /**
     * Constructs a new identifier.
     * @param userId The user identifier to include.
     * @param receivePortIdentifier The receive port to send the result to.
     */
    public TaskIdentifier( Object userId, ReceivePortIdentifier receivePortIdentifier )
    {
        this.id = serialNo++;
        this.userId = userId;
        this.receivePort = receivePortIdentifier;
    }

    /** Reports the result of the task back to the original submitter.
     * 
     * @param node The node we're running on.
     * @param result The result to report.
     */
    public void reportResult( Node node, JobResultValue result )
    {
        node.sendResultMessage( receivePort, this, result );
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + (int) (id ^ (id >>> 32));
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final TaskIdentifier other = (TaskIdentifier) obj;
        if (id != other.id)
            return false;
        return true;
    }

}
