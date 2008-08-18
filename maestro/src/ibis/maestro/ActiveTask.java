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
    final NodeTaskInfo workerTaskInfo;

    /** The time this task was sent to the worker. */
    final long startTime;

    /** The predicted duration of the task. */
    final long predictedDuration;

    private long allowanceDeadline;
    
    /** The moment this task should be completed. */
    final long rescheduleDeadline;

    ActiveTask( TaskInstance task, long id, long startTime, NodeTaskInfo workerTaskInfo, long predictedDuration, long allowanceDeadline, long rescheduleDeadline )
    {
        this.task = task;
        this.id = id;
        this.workerTaskInfo = workerTaskInfo;
        this.startTime = startTime;
        this.predictedDuration = predictedDuration;
        this.allowanceDeadline = allowanceDeadline;
        this.rescheduleDeadline = rescheduleDeadline;
    }

    /**
     * Returns allowanceDeadline.
     * @return allowanceDeadline.
     */
    long getAllowanceDeadline()
    {
        return allowanceDeadline;
    }

    /**
     * Assign a new value to allowanceDeadline.
     * @param allowanceDeadline The new value for allowanceDeadline.
     */
    void setAllowanceDeadline( long allowanceDeadline )
    {
        this.allowanceDeadline = allowanceDeadline;
    }

    /**
     * Returns rescheduleDeadline.
     * @return rescheduleDeadline.
     */
    long getRescheduleDeadline()
    {
        return rescheduleDeadline;
    }

    /**
     * Returns a string representation of this task queue entry.
     * @return The string.
     */
    @Override
    public String toString() {
	return "(ActiveTask id=" + id + ", task=" + task + ", start time " + Utils.formatNanoseconds( startTime ) + ')';
    }
  
 }
