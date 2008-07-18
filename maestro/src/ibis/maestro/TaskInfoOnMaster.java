package ibis.maestro;

import java.util.LinkedList;

/**
 * Information on a task on the master.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskInfoOnMaster
{
    private LinkedList<WorkerTaskInfo> workers = new LinkedList<WorkerTaskInfo>();
    final TaskType type;

    TaskInfoOnMaster( TaskType type )
    {
        this.type = type;
    }

    /**
     * Returns a string representation of this task info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return String.format( "type=" + type + " workers: ", workers.size() );
    }

    /**
     * Add a new worker to our list of supporting workers.
     * We place it in front of the list to give it a chance to do work.
     * @param worker The worker to add.
     */
    protected void addWorker( WorkerTaskInfo worker )
    {
	workers.add( 0, worker );
    }

    protected WorkerTaskInfo getBestWorker()
    {
        if( Settings.useShuffleRouting ) {
            for( WorkerTaskInfo wi: workers ) {
                if( wi.isReady() ) {
                    return wi;
                }
            }
        }
        else {
            WorkerTaskInfo best = null;
            long bestInterval = Long.MAX_VALUE;
            int competitors = 0;
            int idleWorkers = 0;

            for( int i=0; i<workers.size(); i++ ) {
                WorkerTaskInfo wi = workers.get( i );
                WorkerInfo worker = wi.worker;

                if( !worker.isDead() ) {
                    if( wi.isIdle() ) {
                        idleWorkers++;
                    }
                    competitors++;
                    long val = wi.estimateJobCompletion();

                    if( val<Long.MAX_VALUE ) {
                        if( val<bestInterval ) {
                            bestInterval = val;
                            best = wi;
                        }
                    }
                }
            }
            if( Settings.traceRemainingJobTime || Settings.traceMasterProgress || Settings.traceWorkerSelection ) {
                for( WorkerTaskInfo wi: workers ) {
                    System.out.print( "Worker for " + type + ":" );
                    WorkerInfo worker = wi.worker;
                    System.out.print( " " + worker + ":" );
                    if( worker.isDead() ) {
                        System.out.print( "DEAD" );
                    }
                    else {
                        long val = wi.estimateJobCompletion();
                        System.out.print( Service.formatNanoseconds( val ) );
                    }
                    if( wi == best ) {
                        System.out.print( "<=" );
                    }
                }
                System.out.println();
            }

            if( best == null ) {
                // We can't find a worker for this task. See if there is
                // a disabled worker we can enable.
                long bestTime = Long.MAX_VALUE;
                WorkerTaskInfo candidate = null;

                for( int i=0; i<workers.size(); i++ ) {
                    WorkerTaskInfo wi = workers.get( i );

                    if( wi.isIdle() ) {
                        long t = wi.getOptimisticRoundtripTime();
                        if( t<bestTime ) {
                            candidate = wi;
                            bestTime = t;
                        }
                    }
                }
                if( candidate != null ) {
                    if( Settings.traceMasterQueue ) {
                        Globals.log.reportProgress( "Trying worker " + candidate + "; taskInfo=" + this );
                    }
                    best = candidate;
                }
            }
            if( best == null ) {
                if( Settings.traceMasterQueue ){
                    int busy = 0;
                    for( WorkerTaskInfo wi: workers ){
                        busy++;
                    }
                    Globals.log.reportProgress( "No best worker (" + busy + " busy) for task of type " + type );
                }
            }
            else {
                if( Settings.traceMasterQueue ){
                    Globals.log.reportProgress( "Selected " + best + " for task of type " + type + "; estimated job completion time " + Service.formatNanoseconds( bestInterval ) + "; taskInfo=" + this );
                }
            }
            return best;            
        }
	return null;
    }

    /** Register the fact that the given worker has completed
     * its task. To reward it, place in the front of the list
     * of ready workers.
     * @param w The worker that completed a task.
     */
    protected void registerWorkerCompleted( WorkerTaskInfo w )
    {
	workers.remove( w );
	workers.add( 0, w );
    }

    /**
     * Returns the best average completion time for this task.
     * We compute this by taking the minimum over all our workers.
     * @return The best average completion time of our workers.
     */
    long getAverageCompletionTime()
    {
        long res = Long.MAX_VALUE;

        for( WorkerTaskInfo wi: workers ) {
            long val = wi.getAverageCompletionTime();

            if( val<res ) {
                res = val;
            }
        }
        return res;
    }
}
