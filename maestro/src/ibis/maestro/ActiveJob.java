package ibis.maestro;

/**
 * An entry in the list of outstanding jobs of a master.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class ActiveJob {
    final JobInstance job;

    final long id;

    final NodeJobInfo nodeJobInfo;

    /** The time this task was sent to the worker. */
    final double startTime;

    /** The predicted duration of the task. */
    final double predictedDuration;

    private double allowanceDeadline;

    /** The moment this job should be completed. */
    final double rescheduleDeadline;

    ActiveJob(JobInstance job, long id, double startTime,
            NodeJobInfo nodeJobInfo, double predictedDuration,
            double allowanceDeadline, double rescheduleDeadline) {
        this.job = job;
        this.id = id;
        this.nodeJobInfo = nodeJobInfo;
        this.startTime = startTime;
        this.predictedDuration = predictedDuration;
        this.allowanceDeadline = allowanceDeadline;
        this.rescheduleDeadline = rescheduleDeadline;
    }

    /**
     * Returns allowanceDeadline.
     * 
     * @return allowanceDeadline.
     */
    double getAllowanceDeadline() {
        return allowanceDeadline;
    }

    /**
     * Assign a new value to allowanceDeadline.
     * 
     * @param allowanceDeadline
     *            The new value for allowanceDeadline.
     */
    void setAllowanceDeadline(double allowanceDeadline) {
        this.allowanceDeadline = allowanceDeadline;
    }

    /**
     * Returns rescheduleDeadline.
     * 
     * @return rescheduleDeadline.
     */
    double getRescheduleDeadline() {
        return rescheduleDeadline;
    }

    /**
     * Returns a string representation of this task queue entry.
     * 
     * @return The string.
     */
    @Override
    public String toString() {
        return "(ActiveJob id=" + id + ", task=" + job + ", start time "
                + Utils.formatSeconds(startTime) + ')';
    }

}
