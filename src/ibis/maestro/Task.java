package ibis.maestro;

/**
 * A task, consisting of a sequence of jobs.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Task {
    private final Node node;
    private TaskIdentifier id;
    private final String name;
    private final Job[] jobs;

    static final class TaskIdentifier {
	final int id;
	
	private TaskIdentifier( int id )
	{
	    this.id = id;
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
    public TaskInstanceIdentifier buildTaskInstanceIdentifier( Object userIdentifier )
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
    private void submit( TaskInstanceIdentifier tii, int jobNo, Object value )
    {
	JobType type = createJobType( jobNo );
	JobInstance j = new JobInstance( tii, type, value );
	node.submit( j );
    }
    
    /**
     * Submits the given input value to the first job of the task.
     * @param value The value to submit.
     * @param listener The completion listener to use.
     */
    public void submit( Object userId, Object value, CompletionListener listener )
    {
	TaskInstanceIdentifier tii = buildTaskInstanceIdentifier( userId );
	node.addRunningTask( tii, listener );
	submit( tii, 0, value );
    }
}
