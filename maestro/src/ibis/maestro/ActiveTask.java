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
    final WorkerTaskInfo workerTaskInfo;

    /** The time this task was sent to the worker. */
    final long startTime;

    /** The moment this task should be completed. */
    final long deadline;

    ActiveTask( TaskInstance task, long id, long startTime, WorkerTaskInfo workerTaskInfo, long deadline )
    {
        this.task = task;
        this.id = id;
        this.workerTaskInfo = workerTaskInfo;
        this.startTime = startTime;
        this.deadline = deadline;
    }

    /**
     * Returns a string representation of this task queue entry.
     * @return The string.
     */
    @Override
    public String toString() {
	return "(ActiveTask id=" + id + ", task=" + task + ", start time " + Service.formatNanoseconds( startTime ) + ')';
    }
  
 }
