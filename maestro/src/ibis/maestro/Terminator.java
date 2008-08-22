package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;

/**
 * A thread that only runs (if at all) on the maestro, and sends stop
 * messages to random non-maestro nodes. Used to test fault-tolerance.
 *
 * @author Kees van Reeuwijk.
 */
class Terminator extends Thread
{
    /** How many nodes should we stop? */
    private int stopQuotum;
    private final boolean gracefully;
    private final ArrayList<IbisIdentifier> victims = new ArrayList<IbisIdentifier>();
    private int failedMessageCount = 0;
    private int messageCount = 0;
    private final int initialSleepTime; // Time in ms before the first stop message.
    private final int sleepTime; // Time in ms between stop messages.

    Terminator( int quotum, int initialSleepTime, int sleepTime, boolean gracefully )
    {
        super( "Maestro Terminator" );
        this.stopQuotum = quotum;
        this.initialSleepTime = initialSleepTime;
        this.sleepTime = sleepTime;
        this.gracefully = gracefully;
        setDaemon( true );
    }

    private void sendStopMessage( IbisIdentifier target )
    {
        long len;
        SendPort port = null;
        StopNodeMessage msg = new StopNodeMessage( gracefully );
        Globals.log.reportProgress( "Sending stop message to " + target + "; gracefully=" + gracefully );
        try {
            port = Globals.localIbis.createSendPort( PacketSendPort.portType );
            port.connect( target, Globals.receivePortName, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT, true );
            WriteMessage writeMessage = port.newMessage();
            try {
                writeMessage.writeObject( msg );
            }
            finally {
                len = writeMessage.finish();
            }
            if( Settings.traceSends ) {
                Globals.log.reportProgress( "Sent stop message of " + len + " bytes: " + msg );
            }
        }
        catch( IOException e ) {
            failedMessageCount++;
        }
        finally {
            try {
                if( port != null ) {
                    port.close();
                }
            }
            catch( Throwable x ) {
                // Nothing we can do.
            }
        }
        stopQuotum--;
        messageCount++;
    }
    
    private boolean stopRandomNode()
    {
	IbisIdentifier victim;

	synchronized( this ) {
	    if( victims.isEmpty() ) {
		return false;
	    }
	    // Draw a random victim from the list.
	    int ix = Globals.rng.nextInt( victims.size() );
	    victim = victims.remove( ix );
	}
	sendStopMessage( victim );
	return true;
    }

    /** Runs this thread. */
    @Override
    public void run()
    {
	long ourSleepTime = initialSleepTime;
	while( stopQuotum>0 ) {
	    long deadline = System.currentTimeMillis() + ourSleepTime;
	    long now;
	    do {
		long waitTime = deadline - System.currentTimeMillis();
		try {
		    if( Settings.traceTerminator ) {
			Globals.log.reportProgress( "Terminator: waiting " + waitTime + " ms" );
		    }
		    synchronized( this ) {
			this.wait( waitTime );
		    }
		} catch (InterruptedException e) {
		    // ignore.
		}
		now = System.currentTimeMillis();
	    } while( now<deadline );
	    stopRandomNode();
	    ourSleepTime = sleepTime;
	}
    }

    synchronized void registerNode( IbisIdentifier ibis )
    {
	if( !ibis.equals( Globals.localIbis.identifier() ) ) {
	    victims.add( ibis );
	}
    }

    synchronized void removeNode( IbisIdentifier ibis )
    {
        victims.remove( ibis );
    }


    void printStatistics( PrintStream s )
    {
        s.println( "Sent " + messageCount + " stop messages, with " + failedMessageCount + " failures" );
    }

}
