package ibis.maestro;

import java.io.IOException;

import ibis.ipl.Ibis;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

/**
 * A worker in the Maestro master-worker system.
 * @author Kees van Reeuwijk
 *
 */
@SuppressWarnings("synthetic-access")
public class Worker implements Runnable {
    private static final PortType jobPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_POLL, PortType.CONNECTION_ONE_TO_ONE );
    private static final PortType resultPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_POLL, PortType.CONNECTION_ONE_TO_ONE );
    private ReceivePort jobPort;
    private SendPort resultPort;
    private SendPort jobRequestPort;
    private static final long BACKOFF_DELAY = 10;  // In ms.
    private boolean stopped;

    private synchronized void setStopped( boolean val ) {
	stopped = val;
    }
    
    private synchronized boolean isStopped() {
	return stopped;
    }

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
        resultPort = ibis.createSendPort(resultPortType, "resultPort" );
    }

    /** Runs this worker.
     */
    public void run()
    {
	try {
	setStopped( false );
	while( !isStopped() ) {
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
	catch( ClassNotFoundException x ) {
	    
	}
	catch( IOException  x ) {
	    
	}
    }
    
    /**
     * Stop this worker.
     */
    public void stop()
    {
	setStopped( true );
    }
}
