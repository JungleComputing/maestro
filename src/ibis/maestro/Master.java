package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.LinkedList;
import java.util.PriorityQueue;

/**
 * A send queue in the Maestro flow graph framework.
 * 
 * @author Kees van Reeuwijk
 * 
 */
@SuppressWarnings("synthetic-access")
public class Master implements Runnable {
    private final WorkerList workers = new WorkerList();
    private final PacketUpcallReceivePort<WorkerMessage> requestPort;
    private final PacketSendPort<MasterMessage> submitPort;
    private final PriorityQueue<JobQueueEntry> queue = new PriorityQueue<JobQueueEntry>();
    private final LinkedList<ActiveJob> activeJobs = new LinkedList<ActiveJob>();
    private CompletionListener completionListener;
    private long jobno = 0;
    private boolean stopped = false;

    private void subscribeWorker( ReceivePortIdentifier worker )
    {
	workers.subscribeWorker( worker );
    }

    private void unsubscribeWorker( ReceivePortIdentifier worker )
    {
	workers.unsubscribeWorker( worker );
    }

    private class JobRequestHandler implements PacketReceiveListener<WorkerMessage> {
        /**
         * Handles job request message <code>message</code>.
         * @param result The job request message.
         */
        @Override
        public void packetReceived(PacketUpcallReceivePort<WorkerMessage> p, WorkerMessage msg) {
            if( msg instanceof JobResultMessage ) {
        	JobResultMessage result = (JobResultMessage) msg;
        	long id = result.getId();

        	System.err.println( "Received a job result " + result );
        	ActiveJob e = searchQueueEntry( id );
        	if( e == null ) {
        	    System.err.println( "Internal error: ignoring reported result from job with unknown id " + id );
        	    return;
        	}
        	completionListener.jobCompleted( e.getJob(), result.getResult() );
                WorkerInfo worker = e.getWorker();
                long now = System.nanoTime();
                worker.registerJobCompletionTime( now, result.getComputeTime() );
        	synchronized( activeJobs ) {
        	    activeJobs.remove( e );
        	    this.notify();   // Wake up master thread; we might have stopped.
        	}
            }
            else if( msg instanceof WorkRequestMessage ) {
        	WorkRequestMessage m = (WorkRequestMessage) msg;

        	subscribeWorker( m.getPort() );
            }
            else if( msg instanceof WorkerResignMessage ) {
        	WorkerResignMessage m = (WorkerResignMessage) msg;

        	unsubscribeWorker( m.getPort() );
            }
            else {
        	System.err.println( "TODO: the master should handle message of type " + msg.getClass() );
            }
        }
    }

    /** Creates a new master instance.
     * @param ibis The Ibis instance this master belongs to.
     * @param l The completion listener to use.
     * @throws IOException Thrown if the master cannot be created.
     */
    public Master( Ibis ibis, CompletionListener l ) throws IOException
    {
        completionListener = l;
        submitPort = new PacketSendPort<MasterMessage>( ibis );
        requestPort = new PacketUpcallReceivePort<WorkerMessage>( ibis, "requestPort", new JobRequestHandler() );
        requestPort.enable();
    }

    /**
     * Given a job identifier, returns the job queue entry with that id, or null.
     * @param id The job identifier to search for.
     * @return The JobQueueEntry of the job with this id, or null if there isn't one.
     */
    private ActiveJob searchQueueEntry( long id )
    {
        // Note that we blindly assume that there is only one entry with
        // the given id. Reasonable because we hand out the ids ourselves...
        synchronized( activeJobs ) {
            for( ActiveJob e: activeJobs ) {
                if( e.getId() == id ) {
                    return e;
                }
            }
        }
        return null;
    }

    private synchronized void setStopped( boolean val ) {
	stopped = val;
	this.notify();
    }

    private synchronized boolean isStopped()
    {
	return stopped;
    }

    /**
     * Adds the given job to the work queue of this master.
     * Note that the master uses a priority queue for its scheduling,
     * so jobs may not be executed in chronological order.
     * @param j The job to add to the queue.
     */
    public void submit( Job j ){
        long id;

        synchronized( this ) {
            id = jobno++;
        }
        System.err.println( "Submitting job " + id );
        JobQueueEntry e = new JobQueueEntry( j, id, requestPort.identifier() );
        synchronized( queue ) {
            queue.add( e );
        }
    }

    /**
     * Registers a completion listener with this master.
     * @param l The completion listener to register.
     */
    public synchronized void setCompletionListener( CompletionListener l )
    {
	completionListener = l;
    }
    
    /**
     * Returns true iff there are active jobs.
     * @return True iff there are active jobs.
     */
    private boolean areActiveJobs()
    {
        boolean res;

        synchronized( activeJobs ){
            res = !activeJobs.isEmpty();
        }
        return res;
    }
    
    private boolean areWaitingJobs()
    {
	boolean res;
	
	synchronized( queue ) {
	    res = !queue.isEmpty();
	}
	return res;
    }

    /**
     * As long as there are jobs in the queue and ready workers, send jobs to
     * those workers.
     */
    private void startJobs()
    {
	while( areWaitingJobs() ) {
	    WorkerInfo worker = workers.getFastestWorker();
	    if( worker == null ) {
		// FIXME: advertise for new workers.
		return;
	    }
	    JobQueueEntry e;
	    synchronized( queue ) {
		e = queue.remove();
	    }
	    Job job = e.getJob();
	    long id = jobno++;
	    long startTime = System.nanoTime();
	    RunJobMessage msg = new RunJobMessage( job, id, requestPort.identifier() );
	    ActiveJob j = new ActiveJob( job, id, startTime, worker );
	    synchronized( activeJobs ) {
		activeJobs.add( j );
	    }
	    try {
		submitPort.send( msg, worker.getPort() );
	    } catch (IOException e1) {
		System.err.println( "FIXME: could not send job to " + worker + ": put toothpaste back in the tube" );
		// TODO: Auto-generated catch block
		e1.printStackTrace();
	    }
	}
    }

    /** Runs this master. */
    @Override
    public void run() {
	// We simply wait until we have reached the stop state.
	// and there are no outstanding jobs.
	while( !isStopped() && !areActiveJobs() ) {
	    try {
		wait();
	    }
	    catch( InterruptedException x ) {
		// Nothing to do.
	    }
	}
	try {
	    requestPort.close();
	}
	catch( IOException x ) {
	    // Nothing we can do about it.
	}
    }
    
    /** Stops  this master.   */
    public void stop()
    {
	setStopped( true );
    }
}
