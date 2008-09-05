package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * A thread that gossips with the other ibises to exchange performance information.
 * The strategy is to try and keep recent information on all nodes by exchanging
 * information with all other nodes.
 *
 * @author Kees van Reeuwijk.
 */
class Gossiper extends Thread
{
    private boolean stopped = false;
    private final GossipNodeList nodes = new GossipNodeList();
    private final Gossip gossip = new Gossip();
    private final UpDownCounter gossipQuotum; 
    private final Counter messageCount = new Counter();
    private final Counter gossipItemCount = new Counter();
    private final Counter newGossipItemCount = new Counter();
    private final Counter failedGossipMessageCount = new Counter();
    private final LinkedList<IbisIdentifier> nodesToReplyTo = new LinkedList<IbisIdentifier>();
    private final PacketSendPort sendPort;
    private long adminTime = 0;
    private long sendTime = 0;
    private long sentBytes = 0;

    Gossiper( PacketSendPort sendPort, boolean isMaestro )
    {
	super( "Maestro gossiper thread" );
	this.sendPort = sendPort;
	this.gossipQuotum = new UpDownCounter( isMaestro?40:4 );
	setDaemon( true );
    }

    NodePerformanceInfo[] getGossipCopy()
    {
	return gossip.getCopy();
    }

    private void sendGossip( IbisIdentifier target, boolean needsReply )
    {
	long len;
	SendPort port = null;
	GossipMessage msg = gossip.constructMessage( target, needsReply );
	IbisIdentifier theIbis = msg.destination;
	long startTime = System.nanoTime();
	msg.sendMoment = startTime;
	if( Settings.traceGossip ){
	    Globals.log.reportProgress( "Sending gossip message to " + target + "; needsReply=" + needsReply );
	    if( target.equals( Globals.localIbis.identifier() ) ){
		Globals.log.reportInternalError( "Sending gossip to ourselves" );
	    }
	}
	if( !sendPort.sendNonessentialMessage( target, msg ) ) {
	    try {
		port = Globals.localIbis.createSendPort( PacketSendPort.portType );
		port.connect( theIbis, Globals.receivePortName, Settings.OPTIONAL_COMMUNICATION_TIMEOUT, false );
		long setupTime = System.nanoTime();
		WriteMessage writeMessage = port.newMessage();
		try {
		    writeMessage.writeObject( msg );
		}
		finally {
		    len = writeMessage.finish();
		}
		long stopTime = System.nanoTime();
		if( Settings.traceSends ) {
		    Globals.log.reportProgress( "Sent gossip message of " + len + " bytes in " + Utils.formatNanoseconds(stopTime-setupTime) + "; setup time " + Utils.formatNanoseconds(setupTime-startTime) + ": " + msg );
		}
		synchronized( this ) {
		    adminTime += (setupTime-startTime);
		    sendTime += (stopTime-setupTime);
		    if( len>0 ) {
			sentBytes += len;
		    }
		}
	    }
	    catch( IOException e ) {
		failedGossipMessageCount.add();
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
	}
	gossipQuotum.down();
	messageCount.add();
    }

    private void sendCurrentGossipMessages()
    {

	if( gossip.isEmpty() ){
	    // There is nothing in the gossip yet, don't waste
	    // our quotum.
	    return;
	}
	while( gossipQuotum.isAbove( 0 ) ) {
	    long now = System.currentTimeMillis();
	    IbisIdentifier target = nodes.getStaleNode( now );
	    if( target == null ) {
		// Nobody to gossip. Stop.
		break;
	    }
	    if( isStopped() ) {
		return;
	    }
	    sendGossip( target, true );
	}
    }

    private long computeWaitTimeInMilliseconds()
    {
	if( gossipQuotum.isAbove( 0 ) ) {
	    return nodes.computeWaitTimeInMilliseconds();
	}
	return 0;
    }

    /** Runs this thread. */
    @Override
    public void run()
    {
	while( !isStopped() ) {
	    drainReplyQueue();
	    sendCurrentGossipMessages();
	    long waittime = computeWaitTimeInMilliseconds();
	    // We are not supposed to wait if there are
	    // still messages in the current list, but a perverse
	    // scheduler might have allowed a thread to add to the
	    // queue after our last check.
	    // We don't have to check the future queue, since only
	    // sendAMessage can add to it, and if we miss a deadline
	    // of a waiting message we won't do much damage.
	    try {
		if( Settings.traceGossip ) {
		    if( waittime == 0 ) {
			Globals.log.reportProgress( "Gossiper: waiting indefinitely" );
		    }
		    else {
			Globals.log.reportProgress( "Gossiper: waiting " + waittime + " ms" );
		    }
		}
		synchronized( this ) {
		    this.wait( waittime );
		}
	    } catch (InterruptedException e) {
		// ignore.
	    }
	}
    }

    void registerNode( IbisIdentifier ibis )
    {
	if( ibis.equals( Globals.localIbis.identifier() ) ) {
	    // I'm not going to gossip to myself.
	    return;
	}
	nodes.add( ibis );
	gossipQuotum.up( 2 );
	synchronized( this ) {
	    this.notifyAll();
	}
    }

    void removeNode( IbisIdentifier ibis )
    {
	nodes.remove( ibis );
	gossip.removeInfoForNode( ibis );
	synchronized( this ) {
	    this.notifyAll();
	}
    }

    boolean registerGossip( NodePerformanceInfo update, IbisIdentifier source )
    {
	gossipItemCount.add();
	boolean isnew = gossip.register( update );
	if( isnew ) {
            gossipQuotum.up( 2 );
	    if( update.source.equals( Globals.localIbis.identifier() ) ) {
		// Somebody sent us info about ourselves, if it's too old,
		// send that node an update,
		long staleness = gossip.getLocalTimestamp()-update.timeStamp;
		if( staleness>Settings.GOSSIP_EXPIRATION_IN_CLUSTER && source != null ) {
		    nodes.needsUrgentUpdate( source );
		    gossipQuotum.up();
		}
	    }
	    else {
		newGossipItemCount.add();
		nodes.hadRecentUpdate( update.source );
	    }
	    synchronized( this ) {
	        this.notifyAll();
	    }
	}
	return isnew;
    }

    void recomputeCompletionTimes( long masterQueueIntervals[], JobList jobs, HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap )
    {
	gossip.recomputeCompletionTimes( masterQueueIntervals, jobs, localNodeInfoMap );
    }

    void addQuotum()
    {
	gossipQuotum.up();
	synchronized( this ) {
	    notifyAll();
	}
    }

    void printStatistics( PrintStream s )
    {
	s.println( "Sent " + messageCount.get() + " gossip messages, with " + failedGossipMessageCount.get() + " failures, received " + gossipItemCount.get()  + " gossip items, " + newGossipItemCount.get() + " new" );
	s.println( "Sent " + Utils.formatByteCount( sentBytes ) + " in " + Utils.formatNanoseconds( sendTime ) + ", administration time " + Utils.formatNanoseconds( adminTime ) );
	gossip.print( s );
    }

    /**
     * Directly sends a gossip message to the given node.
     * @param source The node to send the gossip to.
     */
    void queueGossipReply( IbisIdentifier source )
    {
	if( Settings.traceGossip ){
	    Globals.log.reportProgress( "Send a gossip reply to " + source );
	}
	synchronized( nodesToReplyTo ) {
	    nodesToReplyTo.add( source );
	}
	synchronized( this ) {
	    this.notifyAll();
	}
    }

    private void drainReplyQueue()
    {
	while( true ) {
	    IbisIdentifier target;

	    synchronized( nodesToReplyTo ) {
		if( nodesToReplyTo.isEmpty() ) {
		    return;
		}
		target = nodesToReplyTo.remove();
	    }
	    sendGossip( target, false );
	}
    }

    void registerWorkerQueueInfo( WorkerQueueInfo[] update, int numberOfProcessors )
    {
	gossip.registerWorkerQueueInfo( update, numberOfProcessors );
    }

    NodePerformanceInfo getLocalUpdate( )
    {
	return gossip.getLocalUpdate();
    }

    /**
     * Given a number of nodes to wait for, keep waiting until we have gossip information about
     * at least this many nodes, or until the given time has elapsed.
     * @param n The number of nodes to wait for.
     * @param maximalWaitTime The maximal time in ms to wait for these nodes.
     * @return The actual number of nodes there was information for at the moment we stopped waiting.
     */
    int waitForReadyNodes( int n, long maximalWaitTime )
    {
	return gossip.waitForReadyNodes( n, maximalWaitTime );
    }

    synchronized void setStopped()
    {
	stopped = true;
	notifyAll();
    }

    synchronized boolean isStopped()
    {
	return stopped;
    }

    /** Given a task type, return the estimated completion time of this task.
     * 
     * @param type The task type for which we want to know the completion time.
     * @param submitIfBusy If set, take into consideration processors that
     *    are currently fully occupied with tasks.
     * @param localNodeInfoMap Local knowledge about the different nodes.
     * @return
     */
    private long computeCompletionTime(TaskType type, boolean submitIfBusy,
	    HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap )
    {
	return gossip.computeCompletionTime( type, submitIfBusy, localNodeInfoMap );
    }

    int selectFastestTask(TaskType[] types, boolean submitIfBusy,
	    HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap)
    {
	int bestIx = -1;
	long bestTime = Long.MAX_VALUE;
	for( int ix=0; ix<types.length; ix++ ) {
	    TaskType type = types[ix];
	    long t = computeCompletionTime( type, submitIfBusy, localNodeInfoMap );
	    if( t<bestTime ) {
		bestTime = t;
		bestIx = ix;
	    }
	}
	return bestIx;
    }

}
