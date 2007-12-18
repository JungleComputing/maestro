package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;

/**
 * An entry in our job queue.
 * @author Kees van Reeuwijk
 *
 */
class JobQueueEntry<R> implements Comparable<JobQueueEntry<R>>, Serializable {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final Job<R> job;
    private final long id;
    private ReceivePortIdentifier master;

    JobQueueEntry( Job<R> job, long id, ReceivePortIdentifier master )
    {
        this.job = job;
        this.id = id;
        this.master = master;
    }

    Job<R> getJob() { return job; }
    long getId() { return id; }

    /**
     * Returns a comparison result for this queue entry compared
     * to the given other entry.
     * @param other The other queue entry to compare to.
     */
    @Override
    public int compareTo(JobQueueEntry<R> other) {
        int res = this.job.compareTo( other.job );
        if( res == 0 ) {
            if( this.id<other.id ) {
                res = -1;
            }
            else {
                if( this.id>other.id ) {
                    res = 1;
                }
            }
        }
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
	return "job queue entry [id=" + id + ",job=" + job + "]";
    }
}