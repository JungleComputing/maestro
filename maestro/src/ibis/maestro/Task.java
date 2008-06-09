package ibis.maestro;

import java.io.PrintStream;
import java.io.Serializable;

/**
 * A task, consisting of a sequence of jobs.
 * 
 * @author Kees van Reeuwijk
 *
 */
public final class Task
{
    private final Node node;
    final TaskIdentifier id;
    final String name;
    final Job[] jobs;
    final TimeEstimate taskTime = new TimeEstimate( 0 );

    static final class TaskIdentifier implements Serializable {
        private static final long serialVersionUID = -5895857432770361027L;
        final int id;

        private TaskIdentifier( int id )
        {
            this.id = id;
        }

        /**
         * Returns the hash code of this task.
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
         * Returns true iff the given object is a task identifier that is equal
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
            final TaskIdentifier other = (TaskIdentifier) obj;
            return (id == other.id);
        }

        /**
         * Returns a string representation of this task.
         * @return The string representation.
         */
        @Override
        public String toString()
        {
            return "id=" + id;
        }
    }

    @SuppressWarnings("synthetic-access")
    Task( Node node, int id, String name, Job[] jobs )
    {
        this.node = node;
        this.id = new TaskIdentifier( id );
        this.name = name;
        this.jobs = jobs;
    }

    /**
     * Builds a new identifier containing the given user identifier.
     * @param userIdentifier The user identifier to include in this identifier.
     * @return The newly constructed identifier.
     */
    private TaskInstanceIdentifier buildTaskInstanceIdentifier( Object userIdentifier )
    {
        return new TaskInstanceIdentifier( userIdentifier, node.identifier() );
    }

    private JobType createJobType( int jobNo )
    {
        return new JobType( id, jobNo );
    }

    /**
     * Submits a job for execution. 
     * @param tii The task instance this job belongs to.
     * @param jobNo The sequence number of the job to execute in the list of jobs of a task.
     * @param value The input value of the job.
     */
    private void submitAJob( TaskInstanceIdentifier tii, int jobNo, Object value )
    {
        JobType type = createJobType( jobNo );
        JobInstance j = new JobInstance( tii, type, value );
        node.submit( j );
    }

    /**
     * Submits a task by giving a user-defined identifier, and the input value to the first job of the task.
     * @param value The value to submit.
     * @param userId The identifier for the user of this task.
     * @param listener The listener that should be informed when this task is completed.
     */
    public void submit( Object value, Object userId, CompletionListener listener )
    {
        TaskInstanceIdentifier tii = buildTaskInstanceIdentifier( userId );
        node.addRunningTask( tii, this, listener );
        submitAJob( tii, 0, value );
    }

    /**
     * Returns a string representation of this task.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "(task " + name + " " + id + ")";
    }

    /**
     * Given a job type, return the next one in the tasks sequence.
     * @param jobType The current job type.
     * @return The next job type, or <code>null</code> if there isn't one.
     */
    JobType getNextJobType( JobType jobType )
    {
	if( !id.equals( jobType.task ) ) {
	    Globals.log.reportInternalError( "getNextJobType(): not my task: " + jobType.task );
	    return null;
	}
	if( jobType.jobNo<jobs.length-1 ) {
	    return new JobType( jobType.task, jobType.jobNo+1 );
	}
	return null;
    }

    void registerTaskTime( long taskInterval )
    {
	taskTime.addSample( taskInterval );
    }

    /**
     * Prints some statistics for this task.
     * @param s The stream to print to.
     */
    public void printStatistics( PrintStream s )
    {
	s.println( name + ": " + taskTime.toString() );
    }
}
