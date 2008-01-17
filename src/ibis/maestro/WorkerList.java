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

    void subscribeWorker( ReceivePortIdentifier port )
    {
        WorkerInfo worker = new WorkerInfo( port );
        synchronized( workers ){
            workers.add( worker );
        }
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
     * Returns the estimated time span, in ms, until the first worker should
     * be send its next job.
     * @return The interval in ms to the next useful job submission.
     */
    public long getBusyInterval() {
        long res = Long.MAX_VALUE;
        long now = System.nanoTime();

        synchronized( workers ){
            for( WorkerInfo worker: workers ){
                res = Math.min( res, worker.getBusyInterval( now ) );
            }
        }
        return res;
    }
}
