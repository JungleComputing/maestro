package ibis.maestro;

import java.io.PrintStream;
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
    private int taskCount = 0;
    private long totalWorkTime = 0;        
    private long totalQueueTime = 0;     // Cumulative queue time of all tasks.
    final TimeEstimate averageWorkTime = new TimeEstimate( Service.MILLISECOND_IN_NANOSECONDS );
    final TimeEstimate queueTimePerTask = new TimeEstimate( Service.MILLISECOND_IN_NANOSECONDS );

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
            if( Settings.traceRemainingJobTime || Settings.traceWorkerSelection ) {
                System.out.print( "Worker for " + type + ":" );
                for( NodeTaskInfo wi: workers ) {
                    NodeInfo worker = wi.nodeInfo;
                    System.out.print( " " + worker + "=" );
                    if( worker.isReady() ) {
                        System.out.print( "NOT READY" );
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


    /**
     * Registers the completion of a task of this particular type, with the
     * given queue interval and the given work interval.
     * @param queueTime The time this task spent in the queue.
     * @param workTime The time it took to execute this task.
     */
    void countTask( long workTime )
    {
        taskCount++;
        totalWorkTime += workTime;
        averageWorkTime.addSample( workTime );
    }

    void reportStats( PrintStream out, double workTime )
    {
        double workPercentage = 100.0*(totalWorkTime/workTime);
        if( taskCount>0 ) {
            out.println( "Worker: " + type + ":" );
            out.printf( "    # tasks          = %5d\n", taskCount );
            out.println( "    total work time = " + Service.formatNanoseconds( totalWorkTime ) + String.format( " (%.1f%%)", workPercentage )  );
            out.println( "    queue time/task  = " + Service.formatNanoseconds( totalQueueTime/taskCount ) );
            out.println( "    work time/task   = " + Service.formatNanoseconds( totalWorkTime/taskCount ) );
            out.println( "    aver. dwell time = " + Service.formatNanoseconds( (totalWorkTime+totalQueueTime)/taskCount ) );
        }
        else {
            out.println( "Worker: " + type + " is unused" );
        }
    }

    /**
     * @param queueLength The current length of the work queue for this type.
     * @return The estimated current dwell time on this worker for this task.
     */
    long getEstimatedDwellTime( int queueLength )
    {
        long res = averageWorkTime.getAverage() + queueTimePerTask.getAverage()*queueLength;
        return res;
    }

    /**
     * Update the estimate for the queue time per task.
     * @param v The new value for the queue time per task.
     */
    public void setQueueTimePerTask( long v )
    {
        totalQueueTime += v;
        queueTimePerTask.addSample( v );
    }

    /** Returns the estimated time to compute this task.
     * @return The estimated time.
     */
    long getEstimatedComputeTime()
    {
        return averageWorkTime.getAverage();
    }

}
