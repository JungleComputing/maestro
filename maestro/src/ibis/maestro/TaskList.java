package ibis.maestro;

import java.io.PrintStream;
import java.util.ArrayList;

/**
 * The list of all known tasks of this run.
 *
 * @author Kees van Reeuwijk.
 */
final class TaskList
{
    private final ArrayList<Task> tasks = new ArrayList<Task>();

    /** Add a new tasks to this list.
     * @param task
     */
    void add( Task task )
    {
        tasks.add( task );
    }

    Task get( int i )
    {
        return tasks.get( i );
    }

    int size()
    {
        return tasks.size();
    }

    private Task searchTaskID( Task.TaskIdentifier id )
    {
        for( Task t: tasks ) {
            if( t.id.equals( id ) ) {
                return t;
            }
        }
        return null;
    }

    JobType getPreviousJobType( JobType t )
    {
        Task task = searchTaskID( t.task );
        if( task == null ) {
            Globals.log.reportInternalError( "getPreviousJobType(): job type with unknown task id: " + t );
            return null;
        }
        return task.getPreviousJobType( t );
    }

    void printStatistics( PrintStream s )
    {
        for( Task t: tasks ) {
            t.printStatistics( s );
        }
    }

}
