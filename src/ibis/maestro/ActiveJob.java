package ibis.maestro;

/**
 * An entry in our job queue.
 * @author Kees van Reeuwijk
 *
 */
class ActiveJob implements Comparable<ActiveJob> {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    final Job job;
    final long id;
    final long startTime;
    final long completionTime;

    ActiveJob( Job job, long id, long startTime, long completionTime )
    {
        this.job = job;
        this.id = id;
        this.startTime = startTime;
        this.completionTime = completionTime;
    }

    /**
     * Returns a comparison result for this queue entry compared
     * to the given other entry.
     * @param other The other queue entry to compare to.
     */
    @Override
    public int compareTo(ActiveJob other) {
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

    /**
     * Returns a string representation of this job queue entry.
     * @return The string.
     */
    @Override
    public String toString() {
	return "(ActiveJob id=" + id + ", job=" + job + ", start time " + Service.formatNanoseconds( startTime ) + ')';
    }

    /** Given the estimate completion time of the previous job on the same worker,
     * and given the current time, returns the number of ns from now it will take to complete
     * this job.
     * @param l
     * @param now
     * @return
     */
    public long estimateCompletionInterval(long l, long now) {
        // TODO Auto-generated method stub
        return 0;
    }
 
}