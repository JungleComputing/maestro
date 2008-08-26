package ibis.maestro;

import ibis.maestro.Job.JobIdentifier;
import junit.framework.TestCase;

import org.junit.Test;

/**
 * Tests for the master queue.
 *
 * @author Kees van Reeuwijk.
 */
public class MasterQueueTest extends TestCase
{
    private static void addToQueue( TaskType type, MasterQueue queue, Integer... ids )
    {
        for( Integer id: ids ) {
            JobInstanceIdentifier jobInstance = new JobInstanceIdentifier( id, null, null );
            TaskInstance ti = new TaskInstance( jobInstance, type, 0 );
            queue.add( ti );
        }
    }

    private static void removeFromQueue( MasterQueue queue, Integer... ids )
    {
        for( Integer id: ids ) {
            if( queue.isEmpty() ) {
                fail( "Queue is empty, while I expected " + id );
            }
            TaskInstance ti = queue.remove();

            if( ti.jobInstance.id != id ) {
                fail( "Unexpected task from master queue: " + ti.jobInstance.id + " instead of " + id );
            }
        }
    }

    /** */
    @Test
    public void testAdd()
    {
        JobIdentifier id = null;
        TaskType type = new TaskType( id, 0, 1, false, 0 );
        TaskType l[] = new TaskType[] { type };
        Globals.supportedTaskTypes = l;
        Globals.allTaskTypes = l;
        MasterQueue queue = new MasterQueue( l );

        addToQueue( type, queue, 0 );
        removeFromQueue( queue, 0 );
        if( !queue.isEmpty() ) {
            fail( "Queue should be empty" );
        }

        addToQueue( type, queue, 1, 0 );
        removeFromQueue( queue, 0, 1 );
        if( !queue.isEmpty() ) {
            fail( "Queue should be empty" );
        }

        addToQueue( type, queue, 4, 3, 2 );
        removeFromQueue( queue, 2 );
        addToQueue( type, queue, 0, 1, 5, 6 );
        removeFromQueue( queue, 0, 1, 3, 4, 5, 6 );
        if( !queue.isEmpty() ) {
            fail( "Queue should be empty" );
        }
    }

}
