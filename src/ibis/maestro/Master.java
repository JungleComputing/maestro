package ibis.maestro;

import ibis.ipl.Ibis;

import java.io.IOException;
import java.util.LinkedList;
import java.util.PriorityQueue;

/**
 * A master in the Maestro master/worker framework.
 * 
 * @author Kees van Reeuwijk
 * 
 * @param <R> The result type of the jobs.
 */
@SuppressWarnings("synthetic-access")
public class Master<R> implements Runnable {
    private final PacketUpcallReceivePort<JobRequest> requestPort;
    private final PacketSendPort<JobMessage> submitPort;
    private final PacketUpcallReceivePort<JobResult<R>> resultPort;
    private final PriorityQueue<JobQueueEntry<R>> queue = new PriorityQueue<JobQueueEntry<R>>();
    private final LinkedList<JobQueueEntry<R>> activeJobs = new LinkedList<JobQueueEntry<R>>();
    private CompletionListener<R> completionListener;
    private long jobno = 0;
    private boolean stopped = false;

    private class JobRequestHandler implements PacketReceiveListener<JobRequest> {
        /**
         * Handles job request message <code>request</code>.
         * @param p The port on which the packet was received
         * @param request The job request message
         * @throws ClassNotFoundException Thrown if one of the communicated classes was not found
         */
        public void packetReceived(PacketUpcallReceivePort<JobRequest> p, JobRequest request) {
            System.err.println( "Recieved a job request " + request );
            JobQueueEntry<R> j = getJob();
            try {
        	System.err.println( "Sending job " + j + " to worker " + request.getPort() );
        	RunJobMessage<R> message = new RunJobMessage<R>( j.getJob(), j.getId(), resultPort.identifier() );
                submitPort.send( message, request.getPort() );
                System.err.println( "Job " + j + " has been sent" );
                synchronized( activeJobs ) {
                    activeJobs.add (j );
                }
            } catch (IOException e) {
                // FIXME Is there anything we can do???
                e.printStackTrace();
            }
        }
    }

    /**
     * Given a job identifier, returns the job queue entry with that id, or null.
     * @param id The job identifier to search for.
     * @return The JobQueueEntry of the job with this id, or null if there isn't one.
     */
    private JobQueueEntry<R> searchQueueEntry( long id )
    {
        // Note that we blindly assume that there is only one entry with
        // the given id. Reasonable because we hand out the ids ourselves...
        synchronized( activeJobs ) {
            for( JobQueueEntry<R> e: activeJobs ) {
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

    private class JobResultHandler implements PacketReceiveListener<JobResult<R>> {
        /**
         * Handles job request message <code>message</code>.
         * @param result The job request message.
         */
        @Override
        public void packetReceived(PacketUpcallReceivePort<JobResult<R>> p, JobResult<R> result) {
            long id = result.getId();

            System.err.println( "Received a job result " + result );
            JobQueueEntry<R> e = searchQueueEntry( id );
            if( e == null ) {
                System.err.println( "Internal error: ignoring reported result from job with unknown id " + id );
                return;
            }
            completionListener.jobCompleted( e.getJob(), result.getResult() );
            synchronized( activeJobs ) {
                activeJobs.remove( e );
                this.notify();   // Wake up master thread; we might have stopped.
            }
        }
    }

    /** Creates a new master instance.
     * @param ibis The Ibis instance this master belongs to.
     * @param l The completion listener to use.
     * @throws IOException Thrown if the master cannot be created.
     */
    public Master( Ibis ibis, CompletionListener<R> l ) throws IOException
    {
        completionListener = l;
        submitPort = new PacketSendPort<JobMessage>( ibis );
        /** Enable result port first, to avoid the embarrassing situation that a worker gets a job
         * from us, but can't return the result.
         */
        resultPort = new PacketUpcallReceivePort<JobResult<R>>( ibis, "resultPort", new JobResultHandler() );
        resultPort.enable();
        requestPort = new PacketUpcallReceivePort<JobRequest>( ibis, "requestPort", new JobRequestHandler() );
        requestPort.enable();
    }

    /**
     * Adds the given job to the work queue of this master.
     * Note that the master uses a priority queue for its scheduling,
     * so jobs may not be executed in chronological order.
     * @param j The job to add to the queue.
     */
    public void submit( Job<R> j ){
        long id;

        synchronized( this ) {
            id = jobno++;
        }
        System.err.println( "Submitting job " + id );
        JobQueueEntry<R> e = new JobQueueEntry<R>( j, id, resultPort.identifier() );
        synchronized( queue ) {
            queue.add( e );
        }
    }

    private JobQueueEntry<R> getJob()
    {
        synchronized( queue ) {
            return queue.poll();
        }
    }
    
    private void sendJobKill( JobQueueEntry<R> j )
    {
        // FIXME: implement this.
    }

    /**
     * Kill all queued and running jobs.
     * This does not stop this master, it `just' kills current jobs.
     */
    public void killJobs()
    {
        synchronized( queue ){
            queue.clear();
        }
        // FIXME: take a lock on the activeJobs list.
        for( JobQueueEntry<R> j: activeJobs ){
            sendJobKill( j );
        }
    }

    /**
     * Registers a completion listener with this master.
     * @param l The completion listener to register.
     */
    public synchronized void setCompletionListener( CompletionListener<R> l )
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
