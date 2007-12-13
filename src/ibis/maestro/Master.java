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
public class Master {
    private static PortType requestPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_AUTO_UPCALLS, PortType.CONNECTION_MANY_TO_ONE );
    private static PortType submitPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.CONNECTION_ONE_TO_ONE );
    private static PortType resultPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.CONNECTION_ONE_TO_ONE );
    private final ReceivePort requestPort;
    private final SendPort submitPort;
    private final ReceivePort resultPort;
    private final PriorityQueue<Job> queue = new PriorityQueue<Job>();
    private final LinkedList<CompletionListener> completionListeners = new LinkedList<CompletionListener>();
    private JobRequestHandler jobRequestHandler = new JobRequestHandler();
    private JobResultHandler jobResultHandler = new JobResultHandler();
    private final Ibis theIbis;
    private long jobno = 0;

    private class JobRequestHandler implements MessageUpcall {
	/**
	 * Handles job request message <code>message</code>.
	 * @param message The job request message.
	 */
        public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
            Job j = getJob();
            ReceivePortIdentifier receiver = (ReceivePortIdentifier) message.readObject();
            submitPort.connect(receiver);
            WriteMessage msg = submitPort.newMessage();
            msg.writeObject( resultPort.identifier() );
            if( j == null ){
                msg.writeObject( null );
            }
            else {
        	msg.writeObject( j );
        	msg.writeObject( jobno++ );
            }
            msg.finish();
            submitPort.close();
        }
        
    }

    private class JobResultHandler implements MessageUpcall {
	/**
	 * Handles job request message <code>message</code>.
	 * @param message The job request message.
	 */
        public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
            // FIXME: implement this
        }
        
    }

    /** Creates a new master instance.
     * @param ibis The Ibis instance this master belongs to.
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
	synchronized( queue ) {
	    queue.add( j );
	}
    }
    
    private Job getJob()
    {
	synchronized( queue ) {
	    return queue.remove();
	}
    }

    /**
     * Registers a completion listener with this master.
     * @param l The completion listener.
     */
    public void addCompletionListener( CompletionListener l )
    {
	synchronized( completionListeners ) {
	    completionListeners.add( l );
	}
    }
}
