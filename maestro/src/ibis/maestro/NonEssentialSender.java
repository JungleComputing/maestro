package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Maintain a list of messages to send, and keep trying until they're sent.
 * At each attempt we're not very persistent, because the messages are not
 * essential to the progress of the program.
 *
 * @author Kees van Reeuwijk
 *
 */
class NonEssentialSender extends Thread
{
    /** The messages that should be sent as soon as possible. */
    private final PriorityQueue<NonEssentialMessage> waitingMessages;

    /** The messages that should be sent some time in the future. */
    private final PriorityQueue<NonEssentialMessage> futureMessages;

    private long adminTime = 0;
    private long sendTime = 0;
    private long sentBytes = 0;
    private int sentCount = 0;

    private static final class ReadyComparator implements Comparator<NonEssentialMessage>
    {

	/**
	 * 
	 * @param arg0
	 * @param arg1
	 * @return
	 */
	@Override
	public int compare(NonEssentialMessage arg0, NonEssentialMessage arg1) {
	    if( arg0.tries<arg1.tries ) {
		return -1;
	    }
	    if( arg0.tries>arg1.tries ) {
		return 1;
	    }
	    if( arg0.sendMoment<arg1.sendMoment ) {
		return -1;
	    }
	    if( arg0.sendMoment>arg1.sendMoment ) {
		return 1;
	    }
	    return 0;
	}

    }

    private static final class FutureComparator implements Comparator<NonEssentialMessage>
    {
	/**
	 * 
	 * @param arg0
	 * @param arg1
	 * @return
	 */
	@Override
	public int compare(NonEssentialMessage arg0, NonEssentialMessage arg1) {
	    if( arg0.sendMoment<arg1.sendMoment ) {
		return -1;
	    }
	    if( arg0.sendMoment>arg1.sendMoment ) {
		return 0;
	    }
	    return 0;
	}

    }

    /** Computes the wait interval for a resend of a failed message.
     * 
     * @param tries The number of tries on this message.
     * @return
     */
    private long waitTime( int tries )
    {
	return 10*Service.MILLISECOND_IN_NANOSECONDS*1L<<tries;
    }
    
    void submit( NonEssentialMessage msg )
    {
	synchronized( waitingMessages ) {
	    waitingMessages.add( msg );
	    waitingMessages.notify();
	}
    }

    /**
     * We failed to send the message, put it on the future queue for
     * a later attempt.
     * @param msg The failed message.
     */
    private void addToFutureQueue( NonEssentialMessage msg )
    {
	msg.tries++;
	if( msg.tries<Settings.MAXIMAL_REGISTRATION_TRIES ) {
	    long waitTime = waitTime( msg.tries );
	    msg.sendMoment = System.nanoTime()+waitTime;
	    synchronized( futureMessages ) {
		futureMessages.add( msg );
	    }
	    if( Settings.traceNonEssentialSender ) {
		Globals.log.reportProgress( "Send failure, tries=" + msg.tries + " wait for " + Service.formatNanoseconds( waitTime ) + ": " + msg ); 
	    }
	}
    }

    /** 
     * Tries to send a message to the given ibis and port name.
     * @param theIbis The ibis to send the message to.
     * @param msg The message to send.
     */
    private void tryToSendNonEssential( NonEssentialMessage msg )
    {
        IbisIdentifier theIbis = msg.destination;
        long startTime = System.nanoTime();
        msg.sendMoment = startTime;
        try {
            long len;
            SendPort port = Globals.localIbis.createSendPort( PacketSendPort.portType );
            port.connect( theIbis, Globals.receivePortName, Settings.OPTIONAL_COMMUNICATION_TIMEOUT, false );
            long setupTime = System.nanoTime();
            WriteMessage writeMessage = port.newMessage();
            writeMessage.writeObject( msg );
            len = writeMessage.finish();
            port.close();
            long stopTime = System.nanoTime();
            if( Settings.traceSends ) {
                System.out.println( "Sent non-essential message of " + len + " bytes in " + Service.formatNanoseconds(stopTime-setupTime) + "; setup time " + Service.formatNanoseconds(setupTime-startTime) + ": " + msg );
            }
            synchronized( this ) {
                adminTime += (setupTime-startTime);
                sendTime += (stopTime-setupTime);
                sentCount++;
                if( len>0 ) {
                    sentBytes += len;
                }
            }
        } catch (IOException e) {
            // Don't declare a node dead just because of this small problem.
            // node.setSuspect( theIbis );
            Globals.log.reportError( "Cannot send a non-essential " + msg.getClass() + " message to ibis " + theIbis );
            // e.printStackTrace( Globals.log.getPrintStream() );
            addToFutureQueue( msg );
        }
    }

    private boolean sendAMessage()
    {
	NonEssentialMessage m;
	synchronized( waitingMessages ) {
	    m = waitingMessages.poll();
	}
	if( m == null ) {
	    // Nothing in the queue.
	    return false;
	}
	if( Settings.traceNonEssentialSender ) {
	    Globals.log.reportProgress( "Sending " + m );
	}
	tryToSendNonEssential( m );
	return true;
    }
    
    private long computeWaitTimeInMilliseconds()
    {
	NonEssentialMessage m;

	synchronized( futureMessages ) {
	    m = futureMessages.peek();
	}
	if( m == null ) {
	    // Nothing in future queue, indefinite wait.
	    return 0L;
	}
	return Service.nanosecondsToMilliseconds( m.sendMoment-System.nanoTime() );
    }

    /**
     * Remove any messages from the future queue whose time has arrived,
     * and put them on the current queue. Return <code>true</code>
     * iff something happened.
     * @return True iff something happened.
     */
    private boolean getMessagesFromFutureQueue()
    {
	boolean progress = false;
	while( true ) {
	    NonEssentialMessage m;

	    synchronized( futureMessages ) {
		m = futureMessages.peek();
		if( m == null ) {
		    return progress;
		}
		long now = System.nanoTime();
		if( m.sendMoment>=now ) {
		    if( Settings.traceNonEssentialSender ) {
			Globals.log.reportProgress( "Message " + m + " still has to wait" );
		    }
		    return progress;
		}
		m = futureMessages.poll();
	    }
	    progress = true;
	    synchronized( waitingMessages ) {
		if( Settings.traceNonEssentialSender ) {
		    Globals.log.reportProgress( "Message " + m + " moved to send queue" );
		}
		waitingMessages.add( m );
	    }
	}
    }
    
    private static void removeFromQueue( PriorityQueue<NonEssentialMessage > q, IbisIdentifier ibis )
    {
	// Since we cannot index a priority queue, and since we can't delete
	// from the list we're iterating on, we first build a list of messages
	// to delete, and then delete them...
	
	ArrayList<NonEssentialMessage> l = new ArrayList<NonEssentialMessage>();

	for( NonEssentialMessage m: q ) {
	    if( ibis.equals( m.destination ) ) {
		l.add( m );
	    }
	}
	for( NonEssentialMessage m: l ) {
	    q.remove( m );
	}
    }

    void removeMessagesToIbis( IbisIdentifier ibis )
    {
	synchronized( waitingMessages ) {
	    removeFromQueue( waitingMessages, ibis );
	}
	synchronized( futureMessages ) {
	    removeFromQueue( futureMessages, ibis );
	}
    }

    @SuppressWarnings("synthetic-access")
    NonEssentialSender()
    {
        super( "Non-essential sender" );
	ReadyComparator readyComparator = new ReadyComparator();
	FutureComparator futureComparator = new FutureComparator();
	this.waitingMessages = new PriorityQueue<NonEssentialMessage>( 4, readyComparator );
	this.futureMessages = new PriorityQueue<NonEssentialMessage>( 4, futureComparator );
	this.setDaemon( true );
    }

    /** Runs this thread. */
    @Override
    public void run()
    {
	while( true ) {
	    boolean progress;
	    do {
		progress = getMessagesFromFutureQueue();
		progress |= sendAMessage();
	    } while( progress );
	    long waittime = computeWaitTimeInMilliseconds();
	    synchronized( waitingMessages ) {
		// We are not supposed to wait if there are
		// still messages in the current list, but a perverse
		// scheduler might have allowed a thread to add to the
		// queue after our last check.
		// We don't have to check the future queue, since only
		// sendAMessage can add to it, and if we miss a deadline
		// of a waiting message we won't do much damage.
		if( waitingMessages.isEmpty() ) {
		    try {
			waitingMessages.wait( waittime );
		    } catch (InterruptedException e) {
			// ignore.
		    }
		}
	    }
	}
    }

    synchronized void printStatistics( PrintStream s )
    {
        s.println( "Non-essential sender:" );
        s.printf(  "  sent messages  %5d\n", sentCount );
        s.printf(  "  sent bytes     %5d\n", sentBytes );
        s.println( "  admin time     " + Service.formatNanoseconds( adminTime ) );
        s.println( "  send time      " + Service.formatNanoseconds( sendTime ) );
    }
}
