package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.util.ArrayList;
import java.util.List;

/**
 * The list of workers of a master.
 * 
 * @author Kees van Reeuwijk
 */
public class WorkerList {
    private final ArrayList<WorkerInfo> workers = new ArrayList<WorkerInfo>();

    private static int searchWorker( List<WorkerInfo> workers, ReceivePortIdentifier id ) {
	for( int i=0; i<workers.size(); i++ ) {
	    WorkerInfo w = workers.get(i);
	    if( w.hasId( id ) ) {
		return i;
	    }
	}
	return -1;
    }

    /** Estimate the multiplier between benchmark score and compute time of this job,
     * by averaging the multipliers of all known workers.
     */
    double estimateMultiplier( JobType type )
    {
	double sumMultipliers = 0.0;
	int workerCount;
	synchronized( workers ){
	    workerCount = workers.size();

	    for( WorkerInfo w: workers ){
		sumMultipliers += w.calculateMultiplier( type );
	    }
	}
	if( workerCount<1 ){
	    // There are no workers to compare to, so we will have to invent
	    // an estimate. This magic number says that this job is just as
	    // fast as the benchmark run we use to benchmark the nodes.
	    return -1;
	}
	return sumMultipliers/workerCount;
    }

    void subscribeWorker( ReceivePortIdentifier me, ReceivePortIdentifier port, ArrayList<JobType> allowedTypes, int workThreads, long benchmarkComputeTime, long benchmarkRoundtripTime, double benchmarkScore )
    {
	long pingTime = benchmarkRoundtripTime-benchmarkComputeTime;
	WorkerInfo worker = new WorkerInfo( port, workThreads, allowedTypes, benchmarkScore, pingTime );
	if( Settings.writeTrace ) {
	    Globals.tracer.traceWorkerRegistration( me, port, benchmarkScore, benchmarkRoundtripTime, benchmarkComputeTime );
	}
	synchronized( workers ){
	    workers.add( worker );
	}
    }

    void removeWorker( ReceivePortIdentifier worker )
    {
	int i = searchWorker(workers, worker);
	if( i>=0 ) {
	    workers.remove(i);
	}
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Remove any workers on that ibis.
     * @param theIbis The ibis that was gone.
     */
    public void removeWorkers( IbisIdentifier theIbis )
    {
	synchronized( workers ){
	    int ix = workers.size();
	    while( ix>0 ){
		ix--;
		WorkerInfo worker = workers.get( ix );
		if( worker.hasIbis( theIbis ) ){
		    workers.remove( ix );
		}
	    }
	}
    }

    /** Returns true iff we have a worker on our list with the
     * given identifier.
     * @param identifier The identifier to search for.
     * @return True iff we know the given worker.
     */
    public boolean contains( ReceivePortIdentifier identifier )
    {
	int i = searchWorker( workers, identifier );
	return i>=0;
    }

    /**
     * Register a job result in the info of the worker that handled it.
     * @param me Which master am I?
     * @param result The job result.
     * @param completionListener The completion listener that should be notified.
     */
    public void registerJobResult( ReceivePortIdentifier me, JobResultMessage result, CompletionListener completionListener )
    {
	int ix = searchWorker( workers, result.source );
	if( ix<0 ) {
	    System.err.println( "Job result from unknown worker " + result.source );
	    return;
	}
	WorkerInfo w = workers.get( ix );
	w.registerJobResult( me, result, completionListener );
    }


    /**
     * Register a job result in the info of the worker that handled it.
     * @param me Which master am I?
     * @param result The job result.
     */
    public void registerWorkerStatus( ReceivePortIdentifier me, WorkerStatusMessage result )
    {
	int ix = searchWorker( workers, result.source );
	if( ix<0 ) {
	    System.err.println( "Job result from unknown worker " + result.source );
	    return;
	}
	WorkerInfo w = workers.get( ix );
	w.registerWorkerStatus( me, result );
    }

    /**
     * Returns true iff all workers in our list are idle.
     * @return True iff all workers in our list are idle.
     */
    public boolean areIdle()
    {
	synchronized( workers ){
	    for( WorkerInfo w: workers ) {
		if( !w.isIdle() ) {
		    return false;
		}
	    }
	}
	return true;
    }

    /**
     * Returns the next time we should wake up to submit a job to a worker.
     * @return The next wakeup time.
     */
    public long getNextSubmissionTime() {
	long res = Long.MAX_VALUE;

	for( WorkerInfo w: workers ) {
	    long t = w.getNextSubmissionTime();
	    if( t<res ) {
		res = t;
	    }
	}
	return res;
    }

    /** 
     * Returns a list of workers who are ready for a new job.
     * @param now The current time.
     * @return The workers.
     */
    public ArrayList<WorkerInfo> getReadyWorkers( long now )
    {
        ArrayList<WorkerInfo> res = new ArrayList<WorkerInfo>();
        for( WorkerInfo w: workers ) {
            long t = w.getNextSubmissionTime();
            if( t<now ) {
                // This worker is ready for a new job.
                res.add( w );
                if( Settings.traceFastestWorker ) {
                    System.out.println( "Worker " + w + " has been ready for a new job for " + Service.formatNanoseconds( now-t ) );
                }
            }
        }
        return res;
    }
}
