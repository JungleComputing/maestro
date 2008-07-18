package ibis.maestro;

import java.util.LinkedList;

/**
 * Information on a task on the master.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskInfoOnMaster
{
    private LinkedList<WorkerInfo> workers = new LinkedList<WorkerInfo>();
    private double researchBudget;

    TaskInfoOnMaster( double budget )
    {
        researchBudget = 2.0 + budget;
    }

    /** Add the given value to the research budget.
     * @param d The value to add.
     */
    void addResearchBudget( double d )
    {
        researchBudget = Math.min( 10, researchBudget+d );
    }
    
    boolean canResearch()
    {
        return researchBudget>=1.0;
    }
    
    void useResearchBudget()
    {
        researchBudget = Math.max( 0.0, researchBudget-1.0 );
    }
    
    /**
     * Returns a string representation of this task info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return String.format( "researchBudget=%2.3f", researchBudget );
    }

    /**
     * Add a new worker to our list of supporting workers.
     * We place it in front of the list to give it a chance to do work.
     * @param worker The worker to add.
     */
    protected void addWorker( WorkerInfo worker )
    {
	workers.add( 0, worker );
    }

    protected WorkerInfo getBestWorker( TaskType type )
    {
        addResearchBudget( Settings.RESEARCH_BUDGET_PER_TASK );
        if( Settings.useShuffleRouting ) {
            for( WorkerInfo wi: workers ) {
                if( wi.isReady( type ) ) {
                    return wi;
                }
            }
        }
        else {
            WorkerInfo best = null;
            long bestInterval = Long.MAX_VALUE;
            int competitors = 0;
            int idleWorkers = 0;

            for( int i=0; i<workers.size(); i++ ) {
                WorkerInfo wi = workers.get( i );

                if( !wi.isDead() ) {
                    if( wi.isIdle( type ) ) {
                        idleWorkers++;
                    }
                    competitors++;
                    long val = wi.estimateJobCompletion( type );

                    if( val<Long.MAX_VALUE ) {
                        if( Settings.traceRemainingJobTime ) {
                            System.out.println( "Worker " + wi + ": task type " + type + ": estimated completion time " + Service.formatNanoseconds( val ) );
                        }
                        if( val<bestInterval ) {
                            bestInterval = val;
                            best = wi;
                        }
                    }
                }
            }
            if( Settings.traceRemainingJobTime || Settings.traceMasterProgress ) {
                System.out.println( "Master: competitors=" + competitors + "; taskInfo=" + this );
            }

            if( best == null || (idleWorkers>0 && canResearch()) ) {
                // We can't find a worker for this task. See if there is
                // a disabled worker we can enable.
                long bestTime = Long.MAX_VALUE;
                WorkerInfo candidate = null;

                for( int i=0; i<workers.size(); i++ ) {
                    WorkerInfo wi = workers.get( i );

                    if( wi.isIdle( type ) ) {
                        long t = wi.getOptimisticRoundtripTime( type );
                        if( t<bestTime ) {
                            candidate = wi;
                            bestTime = t;
                        }
                    }
                }
                if( candidate != null && candidate!=best ) {
                    useResearchBudget();
                    if( Settings.traceMasterQueue ) {
                        Globals.log.reportProgress( "Trying worker " + candidate + "; taskInfo=" + this );
                    }
                    best = candidate;
                }
            }
            if( best == null ) {
                if( Settings.traceMasterQueue ){
                    int busy = 0;
                    int notSupported = 0;
                    for( WorkerInfo wi: workers ){
                        if( wi.supportsType( type ) ){
                            busy++;
                        }
                        else {
                            notSupported++;
                        }
                    }
                    Globals.log.reportProgress( "No best worker (" + busy + " busy, " + notSupported + " not supporting) for task of type " + type );
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
    protected void registerWorkerCompleted( WorkerInfo w )
    {
	workers.remove( w );
	workers.add( 0, w );
    }
}
