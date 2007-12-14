package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.LinkedList;
import java.util.PriorityQueue;

/**
 * A master in the Maestro master/worker framework.
 * 
 * @author Kees van Reeuwijk
 *
 */
@SuppressWarnings("synthetic-access")
public class Master implements Runnable {
    private static final PortType requestPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_AUTO_UPCALLS, PortType.CONNECTION_MANY_TO_ONE );
    private static final PortType submitPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.CONNECTION_ONE_TO_ONE );
    private static final PortType resultPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.CONNECTION_ONE_TO_ONE );
    private final ReceivePort requestPort;
    private final SendPort submitPort;
    private final ReceivePort resultPort;
    private final PriorityQueue<JobQueueEntry> queue = new PriorityQueue<JobQueueEntry>();
    private final LinkedList<CompletionListener> completionListeners = new LinkedList<CompletionListener>();
    private JobRequestHandler jobRequestHandler = new JobRequestHandler();
    private JobResultHandler jobResultHandler = new JobResultHandler();
    private final Ibis theIbis;
    private long jobno = 0;

    /**
     * An entry in our job queue.
     * @author Kees van Reeuwijk
     *
     */
    private static class JobQueueEntry implements Comparable<JobQueueEntry>{
	private final Job job;
	private final long id;
	
	JobQueueEntry( Job job, long id )
	{
	    this.job = job;
	    this.id = id;
	}
	
	Job getJob() { return job; }
	long getId() { return id; }

	/**
	 * Returns a comparison result for this queue entry compared
	 * to the given other entry.
	 * @param other The other queue entry to compare to.
	 */
	@Override
	public int compareTo(JobQueueEntry other) {
	    int res = this.job.compareTo( other.job );
	    if( res == 0 ) {
		if( this.id<other.id ) {
		    res = -1;
		}
		else {
		    if( this.id>other.id ) {
			res = 1;
		    }
		}
	    }
	    return res;
	}
    }

    private class JobRequestHandler implements MessageUpcall {
	/**
	 * Handles job request message <code>message</code>.
	 * @param message The job request message.
	 * @throws IOException Thrown if there was some kind of I/O error
	 * @throws ClassNotFoundException Thrown if one of the communicated classes was not found
	 */
	@Override
        public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
            JobQueueEntry j = getJob();
            ReceivePortIdentifier receiver = (ReceivePortIdentifier) message.readObject();
            submitPort.connect(receiver);
            WriteMessage msg = submitPort.newMessage();
            msg.writeObject( resultPort.identifier() );
            if( j == null ){
                msg.writeObject( null );
            }
            else {
        	msg.writeObject( j.getJob() );
        	msg.writeObject( j.getId() );
            }
            msg.finish();
            submitPort.close();
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
	synchronized( queue ) {
	    for( JobQueueEntry e: queue ) {
		if( e.getId() == id ) {
		    return e;
		}
	    }
	}
	return null;
    }

    private class JobResultHandler implements MessageUpcall {
	/**
	 * Handles job request message <code>message</code>.
	 * @param message The job request message.
	 */
	@Override
        public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
	    long id = (Long) message.readObject();

	    JobQueueEntry e = searchQueueEntry( id );
	    if( e == null ) {
		System.err.println( "Internal error: job with unknown id " + id + " reported a result" );
		return;
	    }
	    JobResult result = (JobResult) message.readObject();
	    synchronized( completionListeners) {
		for( CompletionListener l: completionListeners ) {
		    l.jobCompleted( e.getJob(), result );
		}
	    }
	    synchronized( queue ) {
		queue.remove( e );
	    }
        }
    }

    /** Creates a new master instance.
     * @param ibis The Ibis instance this master belongs to.
     * @throws IOException Thrown if the master cannot be created.
     */
    public Master( Ibis ibis ) throws IOException
    {
	this.theIbis = ibis;
        requestPort = ibis.createReceivePort(requestPortType, "requestPort", jobRequestHandler );
        submitPort = theIbis.createSendPort( submitPortType, "jobPort" );
        resultPort = theIbis.createReceivePort( resultPortType, "resultPort", jobResultHandler );
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
	JobQueueEntry e = new JobQueueEntry( j, id );
	synchronized( queue ) {
	    queue.add( e );
	}
    }
    
    private JobQueueEntry getJob()
    {
	synchronized( queue ) {
	    return queue.remove();
	}
    }

    /**
     * Registers a completion listener with this master.
     * @param l The completion listener to register.
     */
    public void addCompletionListener( CompletionListener l )
    {
	synchronized( completionListeners ) {
	    completionListeners.add( l );
	}
    }

    /**
     * Unregisters a completion listener with this master.
     * @param l The completion listener to unregister.
     */
    public void removeCompletionListener( CompletionListener l )
    {
	synchronized( completionListeners ) {
	    completionListeners.remove( l );
	}
    }

    @Override
    public void run() {
	// TODO Auto-generated method stub
	
    }
}
