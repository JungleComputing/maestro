package ibis.maestro;

import java.util.ArrayList;

/**
 * The list of running jobs.
 *
 * @author Kees van Reeuwijk.
 */
public class RunningJobs
{
    private final ArrayList<JobInstanceInfo> runningJobs = new ArrayList<JobInstanceInfo>();
    
    synchronized void add( JobInstanceInfo info )
    {
        runningJobs.add( info );
    }
    
    synchronized JobInstanceInfo remove( JobInstanceIdentifier id )
    {
        for( int i=0; i<runningJobs.size(); i++ ){
            JobInstanceInfo job = runningJobs.get( i );
            if( job.identifier.equals( id ) ){
                long jobInterval = System.nanoTime()-job.startTime;
                job.job.registerJobTime( jobInterval );
                runningJobs.remove( i );
                return job;
            }
        }
        return null;
    }
}
