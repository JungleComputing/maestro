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
    
    /** The time this job was sent to the worker. */
    final long startTime;
    
    /** The estimated time this job will be completed, and
     * the worker thread will be available for the next job.
     */
    private long completionTime;
    
    /** The estimated time this job will arrive back on this master. */
    private long arrivalTime;

    ActiveJob( Job job, long id, long startTime, long completionTime, long arrivalTime )
    {
        this.job = job;
        this.id = id;
        this.startTime = startTime;
        this.completionTime = completionTime;
        this.arrivalTime = arrivalTime;
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

    /**
     * Returns the estimated this job will no longer need a work thread.
     * @param now The current time.
     * @return The estimated completion time.
     */
    public long getCompletionTime(long now) {
	if( now>arrivalTime ) {
	    // Our estimated arrival time was wrong.
	    // We still assume the difference between
	    // Completion and arrival time is correct,
	    // but we essentially assume the job result
	    // will arrive right now.
	    return now-(arrivalTime-completionTime);
	}
	return completionTime;
    }
 
}