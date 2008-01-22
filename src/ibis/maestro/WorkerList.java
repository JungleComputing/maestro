package ibis.maestro;

import java.util.Vector;

import ibis.ipl.IbisIdentifier;
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

    /** Estimate the multiplier between benchmark score and compute time of this job,
     * by averaging the multipliers of all known workers.
     */
    private double estimateMultiplier()
    {
        double sumMultipliers = 0.0;
        int workerCount;
        synchronized( workers ){
            workerCount = workers.size();
            
            for( WorkerInfo w: workers ){
                sumMultipliers += w.calculateMultiplier();
            }
        }
        if( workerCount<1 ){
            // There are no workers to compare to, so we will have to invent
            // an estimate. This magic number says that this job is just as
            // fast as the benchmark run we use to benchmark the nodes.
            return 20.0;
        }
        return sumMultipliers/workerCount;
    }
    void subscribeWorker( ReceivePortIdentifier port, long pingTime, double benchmarkScore )
    {
        long computeTime = (long) (benchmarkScore*estimateMultiplier());
        WorkerInfo worker = new WorkerInfo( port, benchmarkScore, pingTime+computeTime, computeTime );
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

    /**
     * We know the given ibis has disappeared from the computation.
     * Remove any workers on that ibis.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis(IbisIdentifier theIbis) {
        synchronized( workers ){
            int ix = workers.size();
            while( ix>0 ){
                ix--;
                WorkerInfo worker = workers.get(ix);
                if( worker.hasIbis( theIbis ) ){
                    workers.remove(ix);
                }
            }
        }
    }
}
