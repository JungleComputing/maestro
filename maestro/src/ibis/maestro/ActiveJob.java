package ibis.maestro;

/**
 * An entry in the list of active jobs of a master.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class ActiveJob {
    final JobInstance jobInstance;

    final long id;

    final NodeJobInfo nodeJobInfo;

    /** The time this job was sent to the worker. */
    final double startTime;

    final double allowanceDeadline;

    /** The moment this job should be completed. */
    final double rescheduleDeadline;

    ActiveJob(JobInstance jobInstance, long id, double startTime,
            NodeJobInfo nodeJobInfo, double allowanceDeadline,
            double rescheduleDeadline) {
        this.jobInstance = jobInstance;
        this.id = id;
        this.nodeJobInfo = nodeJobInfo;
        this.startTime = startTime;
        this.allowanceDeadline = allowanceDeadline;
        this.rescheduleDeadline = rescheduleDeadline;
    }

    /**
     * Returns a string representation of this job queue entry.
     * 
     * @return The string.
     */
    @Override
    public String toString() {
        return "(ActiveJob id=" + id + ", job=" + jobInstance + ", start time "
                + Utils.formatSeconds(startTime) + ')';
    }

}
