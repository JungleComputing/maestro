package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;

/**
 * FIXME.
 *
 * @author Kees van Reeuwijk.
 */
class Gossiper extends Thread
{
    private final Node node;
    
    private final GossipNodeList nodes = new GossipNodeList();
    private final Gossip gossip = new Gossip();
    private UpDownCounter gossipQuotum; 
    private Counter messageCount = new Counter();

    Gossiper( Node node, boolean isMaestro )
    {
        super( "Gossiper" );
        this.node = node;
        this.gossipQuotum = new UpDownCounter( isMaestro?40:4 );
        setDaemon( true );
    }

    /** FIXME.
     * @param target
     */
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

    private boolean sendCurrentGossipMessages()
    {
        boolean progress = false;
        long now = System.currentTimeMillis();

        while( gossipQuotum.isAbove( 0 ) ) {
            IbisIdentifier target = nodes.getStaleNode( now );
            if( target != null ) {
                sendGossip( target, true );
                gossipQuotum.down();
                progress = true;
            }
        }
        return progress;
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
        while( true ) {
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
    
    void addNode( IbisIdentifier ibis )
    {
        nodes.add( ibis );
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
        boolean isnew = gossip.register( update );
        if( isnew ) {
            nodes.hadRecentUpdate( update.source );
        }
        return isnew;
    }

    boolean registerGossip( NodeUpdateInfo updates[] )
    {
        boolean changed = false;
        for( NodeUpdateInfo update: updates ) {
            changed |= registerGossip( update );
        }
        if( changed ) {
            gossipQuotum.up();
        }
        return changed;
    }

    void printStatistics( PrintStream s )
    {
        s.println( "Sent " + messageCount.get() + " gossip messages" );
    }

    /** FIXME.
     * @param source
     */
    void sendGossipReply( IbisIdentifier source )
    {
        sendGossip( source, false );
    }
}
