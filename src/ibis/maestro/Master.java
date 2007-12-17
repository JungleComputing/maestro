package ibis.maestro;

import ibis.ipl.Ibis;

import java.io.IOException;
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
    private final PacketSendPort<JobQueueEntry<R>> submitPort;
    private final PacketUpcallReceivePort<JobResult<R>> resultPort;
    private final PriorityQueue<JobQueueEntry<R>> queue = new PriorityQueue<JobQueueEntry<R>>();
    private CompletionListener<R> completionListener;
    private long jobno = 0;

    private class JobRequestHandler implements PacketReceiveListener<JobRequest> {
        /**
         * Handles job request message <code>request</code>.
         * @param p The port on which the packet was received.
         * @param request The job request message.
         * @throws ClassNotFoundException Thrown if one of the communicated classes was not found
         */
        public void packetReceived(PacketUpcallReceivePort<JobRequest> p, JobRequest request) {
            System.err.println( "Recieved a job request " + request );
            JobQueueEntry<R> j = getJob();
            try {
                submitPort.send(j, request.getPort());
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
        synchronized( queue ) {
            for( JobQueueEntry<R> e: queue ) {
                if( e.getId() == id ) {
                    return e;
                }
            }
        }
        return null;
    }

    private class JobResultHandler implements PacketReceiveListener<JobResult<R>> {
        /**
         * Handles job request message <code>message</code>.
         * @param message The job request message.
         */
        @Override
        public void packetReceived(PacketUpcallReceivePort<JobResult<R>> p, JobResult<R> result) {
            long id = result.getId();

            System.err.println( "Received a job result " + result );
            JobQueueEntry<R> e = searchQueueEntry( id );
            if( e == null ) {
                System.err.println( "Internal error: job with unknown id " + id + " reported a result" );
                return;
            }
            completionListener.jobCompleted( e.getJob(), result.getResult() );
            synchronized( queue ) {
                queue.remove( e );
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
        requestPort = new PacketUpcallReceivePort<JobRequest>( ibis, "requestPort", new JobRequestHandler() );
        submitPort = new PacketSendPort<JobQueueEntry<R>>( ibis );
        resultPort = new PacketUpcallReceivePort<JobResult<R>>( ibis, "resultPort", new JobResultHandler() );
        completionListener = l;
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
            return queue.remove();
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

    /** Runs this master. */
    @Override
    public void run() {
        // TODO Auto-generated method stub

    }
    
    /** Stop this master.
     * @throws IOException 
     */
    public void stop() throws IOException
    {
	requestPort.close();
    }
}
