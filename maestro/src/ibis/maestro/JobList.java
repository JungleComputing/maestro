package ibis.maestro;

import ibis.maestro.Job.JobIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;

/**
 * The list of all known jobs of this run.
 *
 * @author Kees van Reeuwijk.
 */
public final class JobList
{
    private final ArrayList<Job> jobs = new ArrayList<Job>();
    private final ArrayList<TaskType> taskTypes = new ArrayList<TaskType>();
    private int jobCounter = 0;

    /** Add a new jobs to this list.
     * @param job
     */
    void add( Job job )
    {
        jobs.add( job );
    }

    Job get( int i )
    {
        return jobs.get( i );
    }

    int size()
    {
        return jobs.size();
    }

    private Job searchJobID( Job.JobIdentifier id )
    {
        for( Job t: jobs ) {
            if( t.id.equals( id ) ) {
                return t;
            }
        }
        return null;
    }

    TaskType getPreviousTaskType( TaskType t )
    {
        Job job = searchJobID( t.job );
        if( job == null ) {
            Globals.log.reportInternalError( "getPreviousTaskType(): task type with unknown job id: " + t );
            return null;
        }
        return job.getPreviousTaskType( t );
    }

    void printStatistics( PrintStream s )
    {
        for( Job t: jobs ) {
            t.printStatistics( s );
        }
    }

    /**
     * Register a new job.
     * 
     * @param job The job to register.
     */
    void registerJob( Job job )
    {
        JobIdentifier id = job.id;
        Task tasks[] = job.tasks;

        for( int i=0; i<tasks.length; i++ ){
            Task j = tasks[i];
            if( j.isSupported() ) {
                final TaskType taskType = new TaskType( id, i, (tasks.length-1)-i );
                if( Settings.traceTypeHandling ) {
                    System.out.println( "Node supports task type " + taskType);
                }
                taskTypes.add( taskType );
            }
        }
    }

    /**
     * Creates a job with the given name and the given sequence of tasks.
     * 
     * @param name The name of the job.
     * @param tasks The list of tasks of the job.
     * @return A new job instance representing this job.
     */
    public Job createJob( String name, Task... tasks )
    {
        int jobId = jobCounter++;
        Job job = new Job( jobId, name, tasks );

        jobs.add( job );
        registerJob( job );
        return job;
    }

    /**
     * @return A list of all supported task types.
     */
    public TaskType[] getSupportedTaskTypes()
    {
        TaskType res[] = new TaskType[taskTypes.size()];
        taskTypes.toArray( res );
        return res;
    }
}
