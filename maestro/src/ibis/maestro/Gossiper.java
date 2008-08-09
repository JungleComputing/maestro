package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;

/**
 * A thread that gossips with the other ibises to exchange performance information.
 * The strategy is to try and keep recent information on all nodes by exchanging
 * information with all other nodes.
 * 
 * Since different nodes will have a different idea of time, we timestamp
 * gossip information ourselves. Info from many hops away may in fact
 * be stale, but at least every provider also puts a timestamp on
 * the info, and we will never replace info with older info.
 *
 * @author Kees van Reeuwijk.
 */
class Gossiper extends Thread
{
    private final Node node;
    
    private boolean stopped = false;
    private final GossipNodeList nodes = new GossipNodeList();
    private final Gossip gossip = new Gossip();
    private UpDownCounter gossipQuotum; 
    private Counter messageCount = new Counter();
    private Counter gossipItemCount = new Counter();
    private Counter newGossipItemCount = new Counter();

    Gossiper( Node node, boolean isMaestro )
    {
        super( "Maestro gossiper thread" );
        this.node = node;
        this.gossipQuotum = new UpDownCounter( isMaestro?40:4 );
        setDaemon( true );
    }
    
    NodeUpdateInfo[] getGossipCopy()
    {
        return gossip.getCopy();
    }

    private void sendGossip( IbisIdentifier target, boolean needsReply )
    {
        if( Settings.traceGossip ) {
            Globals.log.reportProgress( "Sending gossip message to " + target + " needsReply=" + needsReply );
        }
        GossipMessage msg = gossip.constructMessage( target, false );
        node.sendNonEssential( msg );
        gossipQuotum.down();
        messageCount.add();
    }

    private void sendCurrentGossipMessages()
    {
        long now = System.currentTimeMillis();

        if( gossip.isEmpty() ){
            // There is nothing in the gossip yet, don't waste
            // our quotum.
            return;
        }
        while( gossipQuotum.isAbove( 0 ) ) {
            IbisIdentifier target = nodes.getStaleNode( now );
            if( target == null ) {
                // Nobody to gossip. Stop.
                break;
            }
            sendGossip( target, true );
        }
    }
    
    private long computeWaitTimeInMilliseconds()
    {
        if( gossipQuotum.isAbove( 0 ) ) {
            return nodes.computeWaitTimeInMilliseconds();
        }
        return Settings.MAXIMUM_GOSSIPER_WAIT;
    }

    /** Runs this thread. */
    @Override
    public void run()
    {
        while( !isStopped() ) {
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
                        if( waittime == 0 ) {
                            Globals.log.reportProgress( "Gossiper: waiting " + waittime + " ms" );
                        }                        
                    }
                }
                synchronized( this ) {
                    this.wait( waittime );
                    gossipQuotum.up();
                }
            } catch (InterruptedException e) {
                // ignore.
            }
        }
    }
    
    void registerNode( IbisIdentifier ibis )
    {
        nodes.add( ibis );
        gossipQuotum.up();
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

    boolean registerGossip( NodeUpdateInfo update )
    {
        gossipItemCount.add();
        boolean isnew = gossip.register( update );
        if( isnew ) {
            newGossipItemCount.add();
            nodes.hadRecentUpdate( update.source );
        }
        return isnew;
    }

    boolean registerGossip( NodeUpdateInfo updates[], IbisIdentifier source )
    {
        boolean changed = false;
        boolean incomplete = updates.length<gossip.size();
        for( NodeUpdateInfo update: updates ) {
            changed |= registerGossip( update );
        }
        if( changed ) {
            gossipQuotum.up();
        }
        if( source != null && (!changed || incomplete) ) {
            nodes.needsUrgentUpdate( source );
        }
        return changed;
    }

    void recomputeCompletionTimes( long masterQueueIntervals[], JobList jobs )
    {
        gossip.recomputeCompletionTimes( masterQueueIntervals, jobs );
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
        s.println( "Sent " + messageCount.get() + " gossip messages, received " + gossipItemCount.get()  + " gossip items, " + newGossipItemCount.get() + " new" );
        gossip.print( s );
    }

    /**
     * Directly sends a gossip message to the given node.
     * @param source The node to send the gossip to.
     */
    void sendGossipReply( IbisIdentifier source )
    {
        sendGossip( source, false );
    }

    void registerWorkerQueueInfo( WorkerQueueInfo[] update, int idleProcessors, int numberOfProcessors )
    {
        gossip.registerWorkerQueueInfo( update, idleProcessors, numberOfProcessors );
    }

    NodeUpdateInfo getLocalUpdate( )
    {
        return gossip.getLocalUpgate();
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
}
