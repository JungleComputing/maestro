package ibis.maestro;

import java.util.Vector;

import ibis.ipl.ReceivePortIdentifier;

/**
 * The list of workers of a master.
 * 
 * @author Kees van Reeuwijk
 */
public class WorkerList {
    private final Vector<WorkerInfo> workers = new Vector<WorkerInfo>();

    private static int searchWorker( Vector<WorkerInfo> workers, ReceivePortIdentifier id ) {
	for( int i=0; i<workers.size(); i++ ) {
	    WorkerInfo w = workers.get(i);
	    if( w.hasId( id ) ) {
		return i;
	    }
	}
	return -1;
    }

    void subscribeWorker( ReceivePortIdentifier worker )
    {
	System.err.println( "FIXME: implement subscribe of worker " + worker );
    }

    void unsubscribeWorker( ReceivePortIdentifier worker )
    {
	int i = searchWorker(workers, worker);
	if( i>=0 ) {
	    workers.remove(i);
	}
	if( Settings.traceWorkerList ) {
	    System.out.println( "unsubscribe of worker " + worker );
	}
    }

    /** Return a worker to execute a job for us.
     * This method may return <code>null</code> if at the moment
     * there is no suitable worker. 
     * @return The worker to execute the job.
     */
    WorkerInfo getFastestWorker()
    {
	long now = System.nanoTime();
	long maxIdle = -1;
	WorkerInfo bestWorker = null;

	synchronized( workers ) {
	    for( WorkerInfo worker: workers ) {
		long idle = worker.getCompletionTime( now );

		if( idle>=0 ) {
		    if( maxIdle<idle ) {
			maxIdle = idle;
			bestWorker = worker;
		    }
		}
	    }
	}
	return bestWorker;
    }
    
    /**
     * Register a completion time for the given worker.
     * @param worker The worker in question.
     * @param completionTime The time in ns that it took to complete its latest job.
     */
    void registerCompletionTime( ReceivePortIdentifier worker, long completionTime )
    {
	System.err.println( "Worker " + worker + " completed its last job in " + completionTime + "ns" );
    }
}
