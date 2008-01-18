package ibis.maestro;

/**
 * An entry in our job queue.
 * @author Kees van Reeuwijk
 *
 */
class ActiveJob implements Comparable<ActiveJob> {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final Job job;
    private final long id;
    private final long startTime;
    private final WorkerInfo worker;

    ActiveJob( Job job, long id, long startTime, WorkerInfo worker )
    {
        this.job = job;
        this.id = id;
        this.startTime = startTime;
        this.worker = worker;
    }

    Job getJob() { return job; }
    long getId() { return id; }

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
	return "(ActiveJob id=" + id + ",job=" + job + ')';
    }

    /** Returns the worker this job belongs to.
     * @return The worker of this job.
     */
    public WorkerInfo getWorker() {
        return worker;
    }
    
    /** Returns the starting time in ns of this active job.
     * @return The starting time.
     */
    public long getStartTime() {
	return startTime;
    }
}