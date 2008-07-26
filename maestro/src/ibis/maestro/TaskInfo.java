package ibis.maestro;

import java.util.LinkedList;

/**
 * Information on a task on the master.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskInfo
{
    private LinkedList<NodeTaskInfo> workers = new LinkedList<NodeTaskInfo>();
    final TaskType type;

    TaskInfo( TaskType type )
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
    protected void addWorker( NodeTaskInfo worker )
    {
	workers.add( worker );
    }

    protected NodeTaskInfo getBestWorker()
    {
        if( Settings.useShuffleRouting ) {
            for( NodeTaskInfo wi: workers ) {
                NodeInfo worker = wi.nodeInfo;
                
                if( worker.isReady() && wi.canProcessNow() ) {
                    return wi;
                }
            }
        }
        else {
            NodeTaskInfo best = null;
            long bestInterval = Long.MAX_VALUE;
            boolean readyWorker = false;   // Is there any worker prepared to do work right now?

            for( int i=0; i<workers.size(); i++ ) {
                NodeTaskInfo wi = workers.get( i );
                NodeInfo worker = wi.nodeInfo;

                if( worker.isReady() ) {
                    long val = wi.estimateJobCompletion();

                    if( wi.canProcessNow() ) {
                        readyWorker = true;
                    }
                    if( val<Long.MAX_VALUE ) {
                        if( val<bestInterval ) {
                            bestInterval = val;
                            best = wi;
                        }
                    }
                }
            }
            if( Settings.traceRemainingJobTime || Settings.traceMasterProgress || Settings.traceWorkerSelection ) {
                System.out.print( "Worker for " + type + ":" );
                for( NodeTaskInfo wi: workers ) {
                    NodeInfo worker = wi.nodeInfo;
                    System.out.print( " " + worker + "=" );
                    if( worker.isDead() ) {
                        System.out.print( "DEAD" );
                    }
                    else {
                        long val = wi.estimateJobCompletion();
                        System.out.print( Service.formatNanoseconds( val ) );
                    }
                    if( wi == best ) {
                        if( wi.canProcessNow() ){
                            System.out.print( "(submit)" );
                        }
                        else {
                            System.out.print( "(reserve)" );
                        }
                    }
                }
                if( readyWorker ) {
                    System.out.println();
                }
                else {
                    System.out.println( "  no worker ready" );
                }
            }

            if( best == null ) {
                if( Settings.traceMasterQueue ){
                    Globals.log.reportProgress( "No workers AT ALL for task of type " + type );
                }
            }
            else {
                if( !readyWorker ) {
                    best = null;
                    if( Settings.traceMasterQueue ){
                        Globals.log.reportProgress( "All workers for task of type " + type + " are busy" );
                    }
                }
                else {
                    if( Settings.traceMasterQueue ){
                        Globals.log.reportProgress( "Selected " + best + " for task of type " + type + "; estimated job completion time " + Service.formatNanoseconds( bestInterval ) + "; taskInfo=" + this );
                    }
                }
            }
            return best;            
        }
	return null;
    }

    /**
     * Returns the best average completion time for this task.
     * We compute this by taking the minimum over all our workers.
     * @return The best average completion time of our workers.
     */
    long getAverageCompletionTime()
    {
        long res = Long.MAX_VALUE;

        for( NodeTaskInfo wi: workers ) {
            long val = wi.getAverageCompletionTime();

            if( val<res ) {
                res = val;
            }
        }
        return res;
    }
}
