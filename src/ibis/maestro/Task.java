package ibis.maestro;

import java.io.Serializable;

/**
 * A task, consisting of a sequence of jobs.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Task {
    private final Node node;
    final TaskIdentifier id;
    final String name;
    final Job[] jobs;

    static final class TaskIdentifier implements Serializable {
        private static final long serialVersionUID = -5895857432770361027L;
        final int id;

        private TaskIdentifier( int id )
        {
            this.id = id;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int PRIME = 31;
            int result = 1;
            result = PRIME * result + id;
            return result;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
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
            if (id != other.id)
                return false;
            return true;
        }
        
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
     * @param listener 
     */
    public void submit( Object value, Object userId, CompletionListener listener )
    {
        TaskInstanceIdentifier tii = buildTaskInstanceIdentifier( userId );
        node.addRunningTask( tii, listener );
        submitAJob( tii, 0, value );
    }
    
    @Override
    public String toString()
    {
        return "(task " + name + " " + id + ")";
    }
}