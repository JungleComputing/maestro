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

    void subscribeWorker( ReceivePortIdentifier me, ReceivePortIdentifier port, ArrayList<JobType> allowedTypes )
    {
	WorkerInfo worker = new WorkerInfo( port, allowedTypes );
	if( Settings.writeTrace ) {
	    Globals.tracer.traceWorkerRegistration( me, port );
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
     * Returns a list of workers who are ready for a new job.
     * @param now The current time.
     * @return The workers.
     */
    public ArrayList<WorkerInfo> getReadyWorkers( long now )
    {
        ArrayList<WorkerInfo> res = new ArrayList<WorkerInfo>();
        for( WorkerInfo w: workers ) {
            if( w.hasSlotAvailable() ) {
                // This worker is ready for a new job.
                res.add( w );
                if( Settings.traceFastestWorker ) {
                    System.out.println( "Worker " + w + " is ready for a new job" );
                }
            }
        }
        return res;
    }
}
