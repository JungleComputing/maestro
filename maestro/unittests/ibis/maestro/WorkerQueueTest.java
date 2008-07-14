package ibis.maestro;

import ibis.maestro.Job.JobIdentifier;
import junit.framework.TestCase;

import org.junit.Test;

/**
 * FIXME.
 *
 * @author Kees van Reeuwijk.
 */
public class WorkerQueueTest extends TestCase
{
    private static void addToQueue( TaskType type, WorkerQueue queue, Integer... ids )
    {
	for( Integer id: ids ) {
	    JobInstanceIdentifier jobInstance = new JobInstanceIdentifier( id, null, null );
	    TaskInstance ti = new TaskInstance( jobInstance, type, 0 );
	    RunTaskMessage msg = new RunTaskMessage( null, null, ti, 0 );
	    queue.add( msg );
	}
    }

    private static void removeFromQueue( WorkerQueue queue, Integer... ids )
    {
	for( Integer id: ids ) {
	    RunTaskMessage msg = queue.remove();
	    
	    if( msg.task.jobInstance.id != id ) {
		fail( "Unexpected task from worker queue: " + msg.taskId + " instead of " + id );
	    }
	}
    }

    /** */
    @Test
    public void testAdd()
    {
	JobIdentifier id = null;
	TaskType type = new TaskType( id, 0, 1, 0 );
	WorkerQueue queue = new WorkerQueue();

	addToQueue( type, queue, 0 );
	removeFromQueue( queue, 0 );

	addToQueue( type, queue, 1, 0 );
	removeFromQueue( queue, 0, 1 );

    }

}
