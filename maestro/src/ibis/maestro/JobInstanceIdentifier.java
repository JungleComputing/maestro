package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;

/**
 * The identifier of a job.
 *
 * @author Kees van Reeuwijk.
 */
class JobInstanceIdentifier implements Serializable
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
    JobInstanceIdentifier( Object userId, ReceivePortIdentifier receivePortIdentifier )
    {
        this.id = serialNo++;
        this.userId = userId;
        this.receivePort = receivePortIdentifier;
    }

    /**
     * Returns a string representation of this job instance identifier.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "(job instance: id=" + id + " user id=" + userId + " port=" + receivePort + ")";
    }
    /**
     * Returns a hash code for this job identifier.
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

    /** Compares this job identifier with the given
     * other object.
     * @param obj The other object to compare to.
     * @return True iff the two job identifiers are equal.
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
        final JobInstanceIdentifier other = (JobInstanceIdentifier) obj;
        if (id != other.id) {
            return false;
        }
        return true;
    }

}
