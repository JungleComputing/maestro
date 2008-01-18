package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;

/**
 * An entry in our job queue.
 * @author Kees van Reeuwijk
 *
 */
class JobQueueEntry implements Comparable<JobQueueEntry>, Serializable {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final Job job;
    private final ReceivePortIdentifier master;

    JobQueueEntry( Job job, ReceivePortIdentifier master )
    {
        this.job = job;
        this.master = master;
    }

    Job getJob() { return job; }

    /**
     * Returns a comparison result for this queue entry compared
     * to the given other entry.
     * @param other The other queue entry to compare to.
     */
    @Override
    public int compareTo(JobQueueEntry other) {
        int res = this.job.compareTo( other.job );
        return res;
    }

    /** Returns the port identifier of the master this job belongs to.
     * @return The port identifier of the master of this job.
     */
    public ReceivePortIdentifier getMaster() {
        return master;
    }
    
    /**
     * Returns a string representation of this job queue entry.
     * @return The string.
     */
    @Override
    public String toString() {
	return "(JobQueueEntry job=" + job + ')';
    }
}