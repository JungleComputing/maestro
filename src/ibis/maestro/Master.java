package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.LinkedList;
import java.util.PriorityQueue;

@SuppressWarnings("synthetic-access")
public class Master {
    private static PortType requestPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_AUTO_UPCALLS, PortType.CONNECTION_MANY_TO_ONE );
    private final ReceivePort requestPort;
    private final PriorityQueue<Job> queue = new PriorityQueue<Job>();
    private final LinkedList<CompletionListener> completionListeners = new LinkedList<CompletionListener>();
    private JobRequestHandler jobRequestHandler = new JobRequestHandler();

    private class JobRequestHandler implements MessageUpcall {

        public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
            Job j = getJob();
            ReceivePortIdentifier receiver = (ReceivePortIdentifier) message.readObject();
            if( j == null ){
                
            }
        }
        
    }

    public Master( Ibis ibis ) throws IOException
    {
        requestPort = ibis.createReceivePort(requestPortType, "requestPort", jobRequestHandler );
    }

    public synchronized void submit( Job j ){
        queue.add( j );
    }
    
    private synchronized Job getJob()
    {
        return queue.remove();
    }

    public synchronized void addCompletionListener( CompletionListener l )
    {
        completionListeners.add( l );
    }
}
