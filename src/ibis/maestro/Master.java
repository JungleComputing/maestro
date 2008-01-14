package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.ReceivePortIdentifier;

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
public class Master implements Runnable {
    private final PacketUpcallReceivePort<JobRequestMessage> requestPort;
    private final PacketSendPort<MasterMessage> submitPort;
    private final PacketUpcallReceivePort<JobResultMessage> resultPort;
    private final PriorityQueue<JobQueueEntry> queue = new PriorityQueue<JobQueueEntry>();
    private final LinkedList<JobQueueEntry> activeJobs = new LinkedList<JobQueueEntry>();
    private CompletionListener completionListener;
    private long jobno = 0;
    private boolean stopped = false;

    private class JobRequestHandler implements PacketReceiveListener<JobRequestMessage> {
        /**
         * Handles job request message <code>request</code>.
         * @param p The port on which the packet was received
         * @param request The job request message
         * @throws ClassNotFoundException Thrown if one of the communicated classes was not found
         */
        public void packetReceived(PacketUpcallReceivePort<JobRequestMessage> p, JobRequestMessage request) {
            System.err.println( "Recieved a job request " + request );
            JobQueueEntry j = getJob();
            try {
        	ReceivePortIdentifier worker = request.getPort();
            System.err.println( "Sending job " + j + " to worker " + worker );
        	RunJobMessage message = new RunJobMessage( j.getJob(), j.getId(), resultPort.identifier() );
                submitPort.send( message, worker );
                System.err.println( "Job " + j + " has been sent" );
                j.setWorker( worker );
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
    private JobQueueEntry searchQueueEntry( long id )
    {
        // Note that we blindly assume that there is only one entry with
        // the given id. Reasonable because we hand out the ids ourselves...
        synchronized( activeJobs ) {
            for( JobQueueEntry e: activeJobs ) {
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

    private class JobResultHandler implements PacketReceiveListener<JobResultMessage> {
        /**
         * Handles job request message <code>message</code>.
         * @param result The job request message.
         */
        @Override
        public void packetReceived(PacketUpcallReceivePort<JobResultMessage> p, JobResultMessage result) {
            long id = result.getId();

            System.err.println( "Received a job result " + result );
            JobQueueEntry e = searchQueueEntry( id );
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
    public Master( Ibis ibis, CompletionListener l ) throws IOException
    {
        completionListener = l;
        submitPort = new PacketSendPort<MasterMessage>( ibis );
        /** Enable result port first, to avoid the embarrassing situation that a worker gets a job
         * from us, but can't return the result.
         */
        resultPort = new PacketUpcallReceivePort<JobResultMessage>( ibis, "resultPort", new JobResultHandler() );
        resultPort.enable();
        requestPort = new PacketUpcallReceivePort<JobRequestMessage>( ibis, "requestPort", new JobRequestHandler() );
        requestPort.enable();
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
        JobQueueEntry e = new JobQueueEntry( j, id, resultPort.identifier() );
        synchronized( queue ) {
            queue.add( e );
        }
    }

    private JobQueueEntry getJob()
    {
        synchronized( queue ) {
            return queue.poll();
        }
    }
    
    private void sendJobKill( JobQueueEntry j )
    {
        long jobid = j.getId();
        ReceivePortIdentifier worker = j.getWorker();
        KillJobMessage msg = new KillJobMessage( jobid, resultPort.identifier() );
        try {
            submitPort.send( msg, worker );
        } catch (IOException e) {
            // Nothing we can do; just ignore it.
        }
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
        for( JobQueueEntry j: activeJobs ){
            sendJobKill( j );
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
