package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.Master.WorkerIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

import java.util.ArrayList;
import java.util.List;

/**
 * The list of workers of a master.
 * 
 * @author Kees van Reeuwijk
 */
public class WorkerList {
    private final ArrayList<WorkerInfo> workers = new ArrayList<WorkerInfo>();

    private static WorkerInfo searchWorker( List<WorkerInfo> workers, WorkerIdentifier workerIdentifier ) {
	for( int i=0; i<workers.size(); i++ ) {
	    WorkerInfo w = workers.get( i );

            System.out.println( "Comparing identifier " + workerIdentifier + " with " + w.identifier + " result: " + w.identifier.equals( workerIdentifier ) );
	    if( w.identifier.equals( workerIdentifier ) ) {
		return w;
	    }
	}
	return null;
    }

    private static WorkerInfo searchWorker( List<WorkerInfo> workers, IbisIdentifier id ) {
        for( int i=0; i<workers.size(); i++ ) {
            WorkerInfo w = workers.get(i);
            if( w.port.ibisIdentifier().equals( id ) ) {
                return w;
            }
        }
        return null;
    }

    private static int searchWorker( List<WorkerInfo> workers, ReceivePortIdentifier id ) {
        for( int i=0; i<workers.size(); i++ ) {
            WorkerInfo w = workers.get(i);
            if( w.port.equals( id ) ) {
                return i;
            }
        }
        return -1;
    }

    WorkerIdentifier subscribeWorker( ReceivePortIdentifier me, ReceivePortIdentifier workerPort, MasterIdentifier identifierForWorker )
    {
        Master.WorkerIdentifier workerID = new Master.WorkerIdentifier( workers.size() );
	WorkerInfo worker = new WorkerInfo( workerPort, workerID, identifierForWorker );

	if( Settings.traceMasterProgress ){
            System.out.println( "Master " + me + ": subscribing worker " + workerID + "; identifierForWorker=" + identifierForWorker );
        }
        workers.add( worker );
        return workerID;
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Remove any workers on that ibis.
     * @param theIbis The ibis that was gone.
     */
    void removeWorker( IbisIdentifier theIbis )
    {
	WorkerInfo wi = searchWorker( workers, theIbis );
	if( wi != null ) {
	    wi.setDead();
	}
    }


    /**
     * We know the given ibis has disappeared from the computation.
     * Remove any workers on that ibis.
     * @param theIbis The worker that is gone.
     */
    void removeWorker( WorkerIdentifier identifier )
    {
        WorkerInfo wi = searchWorker( workers, identifier );
        if( wi != null ) {
            wi.setDead();
        }
    }

    /** Returns true iff we have a worker on our list with the
     * given identifier.
     * @param identifier The identifier to search for.
     * @return True iff we know the given worker.
     */
    public boolean contains( WorkerIdentifier identifier )
    {
	WorkerInfo i = searchWorker( workers, identifier );
	return i != null;
    }

    /**
     * Register a job result in the info of the worker that handled it.
     * @param me Which master am I?
     * @param result The job result.
     */
    public void registerWorkerStatus( ReceivePortIdentifier me, WorkerStatusMessage result )
    {
	WorkerInfo w = searchWorker( workers, result.source );
	if( w == null ) {
	    System.err.println( "Job result from unknown worker " + result.source );
	    return;
	}
	w.registerWorkerStatus( me, result );
    }

    /**
     * Returns true iff all workers in our list are idle.
     * @return True iff all workers in our list are idle.
     */
    public boolean areIdle()
    {
	for( WorkerInfo w: workers ) {
	    if( !w.isIdle() ) {
		return false;
	    }
	}
	return true;
    }

    /**
     * Given a job type, select the best worker from the list that has a
     * free slot. In this context 'best' is simply the worker with the
     * shortest round-trip interval.
     * @param jobType The type of job we want to execute.
     * @return The info of the best worker for this job, or <code>null</code>
     *         if there currently aren't any workers for this job type.
     */
    public WorkerInfo selectBestWorker( JobType jobType )
    {
	WorkerInfo best = null;
	long bestInterval = Long.MAX_VALUE;

	for( WorkerInfo wi: workers ){
	    if( !wi.isDead() ) {
		long val = wi.getRoundTripInterval( jobType );

		if( val<bestInterval ) {
		    bestInterval = val;
		    best = wi;
		}
	    }
	}
	if( Settings.traceFastestWorker ){
	    if( best != null ) {
		System.out.println( "Selected worker " + best + " for job of type " + jobType + "; roundTripTime=" + Service.formatNanoseconds( bestInterval ) );
	    }
	}
	return best;
    }

    /**
     * Given a worker, return true iff we know about this worker.
     * @param worker The worker we want to know about.
     * @return True iff this is a known worker.
     */
    public boolean isKnownWorker(ReceivePortIdentifier worker) {
        int ix = searchWorker( workers, worker );
        return ix>=0;
    }

    /**
     * Try to increment the maximal number of outstanding jobs for the given
     * worker for the given job type.
     * @param workerID The worker that should get a higher number of outstanding jobs.
     * @param jobType The job type for which we want to increment.
     * @return True iff we could actually increment the allowance
     *         for the job type.
     */
    public boolean incrementAllowance( WorkerIdentifier workerID, JobType jobType )
    {
	WorkerInfo wi = workers.get( workerID.value );
	return wi.incrementAllowance( jobType );
    }

    /**
     * Register the job types of the given worker.
     * @param worker The worker for which we have job types.
     * @param allowedType The allowed job types for the given worker.
     */
    public void registerWorkerJobTypes(ReceivePortIdentifier worker, JobType allowedType)
    {
        int ix = searchWorker( workers, worker );
        if( ix>=0 ){
            WorkerInfo wi = workers.get( ix );
            wi.updateAllowedTypes( allowedType );
        }
    }

    /** We don't have work in the queue. Reduce all excessive allowances,
     * so that they don't come to haunt us once we get more work.
     */
    public void reduceAllowances()
    {
	for( WorkerInfo wi: workers ) {
	    wi.reduceAllowances();
	}
    }

    /** Given a worker identifier, declare it dead.
     * @param workerID The worker to declare dead.
     */
    public void declareDead( WorkerIdentifier workerID )
    {
        WorkerInfo w = workers.get( workerID.value );
        w.setDead();
    }

    /** Given a new list of allowed types, update our adminstration
     * of the given worker.
     * 
     * @param workerID The id of the worker that supports these types.
     * @param allowedTypes The list of types supported by this worker.
     */
    public void updateJobTypes(WorkerIdentifier workerID, JobType[] allowedTypes)
    {
        WorkerInfo w = workers.get( workerID.value );
        for( JobType t: allowedTypes ){
            w.updateAllowedTypes( t );
        }
    }
}
