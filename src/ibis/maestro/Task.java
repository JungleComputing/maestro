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

    Task( Node node, int id, String name, Job[] jobs )
    {
	this.node = node;
	this.id = new TaskIdentifier( id );
	this.name = name;
	this.jobs = jobs;
    }

    private void submit( TaskInstanceIdentifier tii, Object value, int jobNo )
    {
	Job j = jobs[jobNo];
	
	//node.submit( j, tii, value );
    }
    
    /**
     * Submits the given input value to the first job of the task.
     * @param value The value to submit.
     */
    public void submit( Object userId, Object value )
    {
	//TaskInstanceIdentifier tii = new TaskInstanceIdentifier( userId, node.master.identifier() );
	//submit( tii, value, 0 );
    }
}
