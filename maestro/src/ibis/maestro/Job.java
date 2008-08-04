package ibis.maestro;

import java.io.PrintStream;
import java.io.Serializable;

/**
 * A job, consisting of a sequence of tasks.
 * 
 * @author Kees van Reeuwijk
 *
 */
public final class Job
{
    final JobIdentifier id;
    final String name;
    final Task[] tasks;
    final TaskType[] taskTypes;
    final TimeEstimate jobTime = new TimeEstimate( 0 );
    private static int index = 0;

    static final class JobIdentifier implements Serializable {
        private static final long serialVersionUID = -5895857432770361027L;
        final int id;

        private JobIdentifier( int id )
        {
            this.id = id;
        }

        /**
         * Returns the hash code of this job.
         * @return The hash code.
         */
        @Override
        public int hashCode() {
            return id;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        /**
         * Returns true iff the given object is a job identifier that is equal
         * to this one.
         * @param obj The object to compare to.
         * @return True iff this and the given object are equal.
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final JobIdentifier other = (JobIdentifier) obj;
            return (id == other.id);
        }

        /**
         * Returns a string representation of this job.
         * @return The string representation.
         */
        @Override
        public String toString()
        {
            return "job=" + id;
        }

    }

    private TaskType[] buildTaskTypes( Task[] taskList )
    {
	TaskType res[] = new TaskType[taskList.length];
	for( int i=0; i<taskList.length; i++ ) {
	    res[i] = new TaskType( id, i, (taskList.length-1)-i, index++ );
	}
	return res;
    }

    @SuppressWarnings("synthetic-access")
    Job( int id, String name, Task[] tasks )
    {
        this.id = new JobIdentifier( id );
        this.name = name;
        this.tasks = tasks;
        this.taskTypes = buildTaskTypes( tasks );
    }

    /**
     * Builds a new identifier containing the given user identifier.
     * @param userIdentifier The user identifier to include in this identifier.
     * @return The newly constructed identifier.
     */
    private JobInstanceIdentifier buildJobInstanceIdentifier( Object userIdentifier )
    {
        return new JobInstanceIdentifier( userIdentifier, Globals.localIbis.identifier() );
    }

    /**
     * Submits a task for execution. 
     * @param tii The job instance this task belongs to.
     * @param taskNo The sequence number of the task to execute in the list of tasks of a job.
     * @param value The input value of the task.
     */
    private void submitATask( Node node, JobInstanceIdentifier tii, int taskNo, Object value )
    {
        TaskType type = taskTypes[taskNo];
        TaskInstance j = new TaskInstance( tii, type, value );
        node.submit( j );
    }

    /**
     * Submits a job by giving a user-defined identifier, and the input value to the first task of the job.
     * @param node The node this job should run on.
     * @param value The value to submit.
     * @param userId The identifier for the user of this job.
     * @param listener The listener that should be informed when this job is completed.
     */
    public void submit( Node node, Object value, Object userId, CompletionListener listener )
    {
        JobInstanceIdentifier tii = buildJobInstanceIdentifier( userId );
        node.addRunningJob( tii, this, listener );
        submitATask( node, tii, 0, value );
    }

    /**
     * Returns a string representation of this job.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "(job " + name + " " + id + ")";
    }

    /**
     * Given a task type, return the previous one in the task sequence of this job,
     * or <code>null</code> if there isn't one.
     * @param taskType The current task type.
     * @return The next task type, or <code>null</code> if there isn't one.
     */
    TaskType getPreviousTaskType( TaskType taskType )
    {
        if( !id.equals( taskType.job ) ) {
            Globals.log.reportInternalError( "getPreviousTaskType(): not my job: " + taskType.job );
            return null;
        }
        if( taskType.taskNo>0 ) {
            return taskTypes[taskType.taskNo-1];
        }
        return null;
    }

    /**
     * Given a task type, return the next one in the task sequence of this job,
     * or <code>null</code> if there isn't one.
     * @param taskType The current task type.
     * @return The next task type, or <code>null</code> if there isn't one.
     */
    TaskType getNextTaskType( TaskType taskType )
    {
	if( !id.equals( taskType.job ) ) {
	    Globals.log.reportInternalError( "getNextTaskType(): not my job: " + taskType.job );
	    return null;
	}
	if( taskType.taskNo<tasks.length-1 ) {
	    return taskTypes[taskType.taskNo+1];
	}
	return null;
    }

    void registerJobTime( long jobInterval )
    {
	jobTime.addSample( jobInterval );
    }

    /**
     * Prints some statistics for this job.
     * @param s The stream to print to.
     */
    public void printStatistics( PrintStream s )
    {
	s.println( "Job '" + name + "': " + jobTime.toString() );
    }
}
