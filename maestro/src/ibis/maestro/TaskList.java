package ibis.maestro;

import ibis.maestro.Task.TaskIdentifier;

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
    private final ArrayList<JobType> jobTypes = new ArrayList<JobType>();
    private int taskCounter = 0;

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

    /**
     * Register a new task.
     * 
     * @param task The task to register.
     */
    void registerTask( Task task )
    {
        TaskIdentifier id = task.id;
        Job jobs[] = task.jobs;

        for( int i=0; i<jobs.length; i++ ){
            Job j = jobs[i];
            if( j.isSupported() ) {
                final JobType jobType = new JobType( id, i );
                if( Settings.traceTypeHandling ) {
                    System.out.println( "Node supports job type " + jobType);
                }
                jobTypes.add( jobType );
            }
        }
    }

    /**
     * Creates a task with the given name and the given sequence of jobs.
     * 
     * @param name The name of the task.
     * @param jobs The list of jobs of the task.
     * @return A new task instance representing this task.
     */
    public Task createTask( String name, Job... jobs )
    {
        int taskId = taskCounter++;
        Task task = new Task( taskId, name, jobs );

        tasks.add( task );
        registerTask( task );
        return task;
    }

    /**
     * @return A list of all supported job types.
     */
    public JobType[] getSupportedJobTypes()
    {
        JobType res[] = new JobType[jobTypes.size()];
        jobTypes.toArray( res );
        return res;
    }
}
