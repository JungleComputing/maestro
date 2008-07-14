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
     * TODO: should we really do that?
     * @param worker The worker to add.
     */
    protected void addWorker( WorkerInfo worker )
    {
	workers.add( 0, worker );
    }

    protected WorkerInfo getReadyWorker( TaskType type )
    {
	for( WorkerInfo wi: workers ) {
	    if( wi.isIdle( type ) ) {
		return wi;
	    }
	}
	return null;
    }

    /** Register the fact that the given worker has completed
     * its task. To reward it, place in the front of the list
     * of ready workers.
     * @param w The worker that completed a task.
     */
    protected void registerWorkerCompleted(WorkerInfo w)
    {
	workers.remove( w );
	workers.add( 0, w );
    }
}
