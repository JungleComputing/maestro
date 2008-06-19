package ibis.maestro;

/**
 * An entry in our task queue.
 * @author Kees van Reeuwijk
 *
 */
class ActiveTask {
    final TaskInstance task;
    final long id;
    final WorkerTaskInfo workerTaskInfo;

    /** The time this task was sent to the worker. */
    final long startTime;

    ActiveTask( TaskInstance task, long id, long startTime, WorkerTaskInfo workerTaskInfo )
    {
        this.task = task;
        this.id = id;
        this.workerTaskInfo = workerTaskInfo;
        this.startTime = startTime;
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
