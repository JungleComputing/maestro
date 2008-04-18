package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;

/**
 * The identifier of a task.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskInstanceIdentifier implements Serializable
{
    private static final long serialVersionUID = -7567750999837567234L;
    private static long serialNo = 0;

    final long id;
    final Object userId;
    final ReceivePortIdentifier receivePort;

    /**
     * Constructs a new identifier.
     * @param userId The user identifier to include.
     * @param receivePortIdentifier The receive port to send the result to.
     */
    public TaskInstanceIdentifier( Object userId, ReceivePortIdentifier receivePortIdentifier )
    {
        this.id = serialNo++;
        this.userId = userId;
        this.receivePort = receivePortIdentifier;
    }

    @Override
    public String toString()
    {
        return "(task instance: id=" + id + " user id=" + userId + " port=" + receivePort + ")";
    }
    /**
     * Returns a hash code for this task identifier.
     * 
     * @return The hash code of the identifier.
     */
    @Override
    public int hashCode()
    {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + (int) (id ^ (id >>> 32));
        return result;
    }

    /** Compares this task identifier with the given
     * other object.
     * @param obj The other object to compare to.
     * @return True iff the two task identifiers are equal.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TaskInstanceIdentifier other = (TaskInstanceIdentifier) obj;
        if (id != other.id) {
            return false;
        }
        return true;
    }

}
