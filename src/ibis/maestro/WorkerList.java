package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.Master.WorkerIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

import java.io.PrintStream;
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
    boolean contains( WorkerIdentifier identifier )
    {
        WorkerInfo i = searchWorker( workers, identifier );
        return i != null;
    }

    /**
     * Register a job result in the info of the worker that handled it.
     * @param me Which master am I?
     * @param result The job result.
     */
    void registerWorkerStatus( ReceivePortIdentifier me, WorkerStatusMessage result )
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
    boolean areIdle()
    {
        for( WorkerInfo w: workers ) {
            if( !w.isIdle() ) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Given a job type, return true iff there is a worker in this
     * list that can execute a job of this type right now.
     * @param jobType The job type under consideration.
     * @return True iff there is a worker that can execute this type of job right now.
     */
    private boolean canNowExecute( JobType jobType )
    {
	for( WorkerInfo wi: workers ) {
	    if( wi.canNowExecute(jobType)) {
		return true;
	    }
	}
	return true;
    }

    /**
     * Given a job type, select the best worker from the list that has a
     * free slot. In this context 'best' is simply the worker with the
     * shortest round-trip interval. We take into account fast workers that
     * can complete a job earlier even if they are busy right now, but
     * if there are more jobs in the queue we may still want to assign
     * a job to a slower processor that can execute the job right now.
     *  
     * @param jobType The type of job we want to execute.
     * @return The info of the best worker for this job, or <code>null</code>
     *         if there currently aren't any workers for this job type.
     */
    WorkerInfo selectBestWorker( JobType jobType, int queuedJobs )
    {
        WorkerInfo best = null;
        int bestix = 0;
        long bestInterval = Long.MAX_VALUE;
        int reservations[] = new int[workers.size()];

	if( !canNowExecute( jobType ) ) {
	    // All workers are at their maximum load, we may as well give up.
	    return null;
	}
	while( queuedJobs>0 ) {
	    // Keep looking for a job that can be executed now until
	    // all jobs are exhausted, or we have found one.
	    queuedJobs--;
	    for( int i=0; i<workers.size(); i++ ) {
		WorkerInfo wi = workers.get( i );

                if( !wi.isDead() ) {
                    long val = wi.estimateRoundTripInterval( jobType, reservations[i] );

                    if( val<bestInterval ) {
                        bestInterval = val;
                        best = wi;
                        bestix = i;
                    }
                }
            }
            if( best == null || best.canNowExecute( jobType ) ){
		break;
	    }
            // Add a reservation for this worker.
            reservations[bestix]++;
            best = null;
	}
	if( Settings.traceMasterQueue ){
	    if( best == null ) {
		int busy = 0;
		int notSupported = 0;
		for( WorkerInfo wi: workers ){
		    if( wi.supportsType( jobType ) ){
			busy++;
		    }
		    else {
			notSupported++;
		    }
		}
		System.out.println( "No best worker (" + busy + " busy, " + notSupported + " not supporting) for job of type " + jobType );
	    }
	    else {
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
    boolean isKnownWorker( ReceivePortIdentifier worker )
    {
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
    boolean incrementAllowance( WorkerIdentifier workerID, JobType jobType )
    {
        WorkerInfo wi = workers.get( workerID.value );
        return wi.incrementAllowance( jobType );
    }

    /**
     * Register the job types of the given worker.
     * @param worker The worker for which we have job types.
     * @param allowedType The allowed job types for the given worker.
     */
    void registerWorkerJobTypes( ReceivePortIdentifier worker, JobType allowedType )
    {
	int ix = searchWorker( workers, worker );
	if( ix>=0 ){
	    WorkerInfo wi = workers.get( ix );
	    wi.registerAllowedType( allowedType );
	}
    }

    /** Given a worker identifier, declare it dead.
     * @param workerID The worker to declare dead.
     */
    void declareDead( WorkerIdentifier workerID )
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
    void updateJobTypes( WorkerIdentifier workerID, JobType[] allowedTypes )
    {
	WorkerInfo w = workers.get( workerID.value );
	for( JobType t: allowedTypes ){
	    w.registerAllowedType( t );
	}
    }

    /**
     * Return the number of known workers.
     * @return The number of known workers.
     */
    int getWorkerCount()
    {
        return workers.size();
    }

    /**
     * Given a print stream, print some statistics about the workers
     * to this stream.
     * @param out The stream to print to.
     */
    void printStatistics( PrintStream out )
    {
        for( WorkerInfo wi: workers ) {
            wi.printStatistics( out );
        }
    }
}
