package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;

/**
 * The identifier of a particular job instance.
 * 
 * @author Kees van Reeuwijk.
 */
class JobInstanceIdentifier implements Serializable {
    private static final long serialVersionUID = -7567750999837567234L;

    /** The job instance identifiers we hand out. */
    private static long serialNo = 0;

    /** The identifier issued by the master node. */
    final long id;

    /**
     * The identifier the user has added to the job instance when submitting it.
     * We require it to implement serializable to make sure we don't get obscure
     * runtime errors.
     */
    final Serializable userId;

    /** The node to which the final result should be transmitted. */
    final IbisIdentifier resultNode;

    /**
     * Constructs a new identifier.
     * 
     * @param id
     *     The identifier issued by the master.
     * @param userId
     *            The user identifier to include.
     * @param resultNode
     *            The node to send the result to.
     */
    JobInstanceIdentifier(long id, Serializable userId, IbisIdentifier resultNode) {
        this.id = id;
        this.userId = userId;
        this.resultNode = resultNode;
    }

    /**
     * Constructs a new identifier.
     * 
     * @param userId
     *            The user identifier to include.
     * @param resultNode
     *            The node to send the result to.
     */
    JobInstanceIdentifier(Serializable userId, IbisIdentifier resultNode) {
        this(serialNo++, userId, resultNode);
    }

    /**
     * Returns a string representation of this job instance identifier.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(job instance: id=" + id + " user id=" + userId + " port="
                + resultNode + ")";
    }

    /**
     * Returns a hash code for this job identifier.
     * 
     * @return The hash code of the identifier.
     */
    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    /**
     * Compares this job identifier with the given other object.
     * 
     * @param obj
     *            The other object to compare to.
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
        return (id == other.id);
    }

    /**
     * Returns a textual representation of this job.
     * 
     * @return
     */
    String label() {
        return "J" + id;
    }

}
