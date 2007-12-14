package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * An entry in our job queue.
 * @author Kees van Reeuwijk
 *
 */
class JobQueueEntry<R> implements Comparable<JobQueueEntry<R>>{
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

    public ReceivePortIdentifier getMaster() {
        return master;
    }
}