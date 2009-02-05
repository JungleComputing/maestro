package ibis.maestro;

/**
 * An entry in the list of outstanding tasks of a master.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class ActiveTask {
    final TaskInstance task;

    final long id;

    final NodeTaskInfo nodeTaskInfo;

    /** The time this task was sent to the worker. */
    final double startTime;

    /** The predicted duration of the task. */
    final double predictedDuration;

    private double allowanceDeadline;

    /** The moment this task should be completed. */
    final double rescheduleDeadline;

    ActiveTask(TaskInstance task, long id, double startTime,
            NodeTaskInfo workerTaskInfo, double predictedDuration,
            double allowanceDeadline, double rescheduleDeadline) {
        this.task = task;
        this.id = id;
        this.nodeTaskInfo = workerTaskInfo;
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
        return "(ActiveTask id=" + id + ", task=" + task + ", start time "
                + Utils.formatSeconds(startTime) + ')';
    }

}
