package ibis.maestro;

import java.io.IOException;

import ibis.ipl.Ibis;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

@SuppressWarnings("synthetic-access")
public class Worker {
    private static PortType jobPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_POLL, PortType.CONNECTION_ONE_TO_ONE );
    private static PortType resultPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_POLL, PortType.CONNECTION_ONE_TO_ONE );
    private ReceivePort jobPort;
    private SendPort resultPort;
    private SendPort jobRequestPort;
    private static final long BACKOFF_DELAY = 10;  // In ms.

    private void sendWorkRequest() throws IOException
    {
        WriteMessage msg = jobRequestPort.newMessage();
        msg.writeObject( jobPort.identifier() );
    }

    /**
     * Create a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to
     * @throws IOException Thrown if the construction of the worker failed.
     */
    public Worker( Ibis ibis ) throws IOException
    {
        jobPort = ibis.createReceivePort(jobPortType, "jobPort" );
        resultPort = ibis.createSendPort(jobPortType, "resultPort" );
    }

    /** Runs this worker.
     * This method only terminates due to an exception.
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void run() throws IOException, ClassNotFoundException
    {
	while( true ) {
	    ReadMessage msg = jobPort.receive();
            ReceivePortIdentifier master = (ReceivePortIdentifier) msg.readObject();
            Object jobID = msg.readObject();
	    Object job = msg.readObject();
	    msg.finish();
	    if( job == null ) {
		try {
		    // TODO: more sophistication.
		    Thread.sleep( BACKOFF_DELAY );
		}
		catch( InterruptedException e ) {
		    // Somebody woke us, but we don't care.
		}
	    }
	    else {
		Job j = (Job) job;
		JobResult r = j.run();
		resultPort.connect( master );
		WriteMessage reply = resultPort.newMessage();
		reply.writeObject( jobID );
		reply.writeObject( r );
		reply.finish();
		resultPort.close();
	    }
	}
    }
}
