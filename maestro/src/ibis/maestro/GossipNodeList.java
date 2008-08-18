package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.Location;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Maintains a list of nodes to gossip with, and the moment we should try to get an update.
 *
 * @author Kees van Reeuwijk.
 */
class GossipNodeList
{
    @SuppressWarnings("synthetic-access")
    PriorityQueue<GossipNode> nodes = new PriorityQueue<GossipNode>( 4, new GossipNodeComparator() );

    private static final class GossipNode {
        final IbisIdentifier ibis;
        long nextUpdateMoment;    // The moment in time in ms for the next update.
        final long updateInterval;      // Time interval in ms for each new update. 

        private static boolean areInSameCluster( IbisIdentifier a, IbisIdentifier b )
        {
            Location la = a.location();
            Location lb = b.location();
            int nodeLevel = Math.min( la.numberOfLevels(), lb.numberOfLevels() );
            int matchingLevels = la.numberOfMatchingLevels( lb );
            boolean res = matchingLevels>=(nodeLevel-1);
            return res;
        }

        private static long computeUpdateInterval( IbisIdentifier ibis )
        {
            boolean sameCluster = areInSameCluster( Globals.localIbis.identifier(), ibis );
            return sameCluster ? Settings.GOSSIP_EXPIRATION_IN_CLUSTER : Settings.GOSSIP_EXPIRATION_BETWEEN_CLUSTERS;
        }

        GossipNode( IbisIdentifier ibis )
        {
            this.ibis = ibis;
            this.nextUpdateMoment = 0L;
            this.updateInterval = computeUpdateInterval( ibis );
        }

    }

    private static final class GossipNodeComparator implements Comparator<GossipNode>
    {

        /**
         * 
         * @param arg0
         * @param arg1
         * @return <code>-1</code> if node arg0 should have priority over arg1, or <code>1</code> for the reverse.
         */
        @Override
        public int compare( GossipNode arg0, GossipNode arg1 )
        {
            if( arg0.nextUpdateMoment<arg1.nextUpdateMoment ) {
                return -1;
            }
            if( arg0.nextUpdateMoment>arg1.nextUpdateMoment ) {
                return 1;
            }
            // Tie breaker: the one with the shortest distance wins.
            return Utils.rankIbisIdentifiers( Globals.localIbis.identifier(), arg0.ibis, arg1.ibis );
        }

    }

    synchronized IbisIdentifier getStaleNode( long now )
    {
        GossipNode node = nodes.peek();
        if( node != null && node.nextUpdateMoment<=now ) {
            node = nodes.poll();
            node.nextUpdateMoment = now + node.updateInterval;
            nodes.add( node );
            return node.ibis;
        }
        return null;
    }

    long computeWaitTimeInMilliseconds()
    {
        GossipNode node = nodes.peek();
        if( node == null ) {
            return 0l;  // No nodes to gossip with. Wait indefinitely.
        }
        return Math.max( 1, node.nextUpdateMoment-System.currentTimeMillis() );
    }

    synchronized void add( IbisIdentifier ibis )
    {
        if( ibis.equals( Globals.localIbis.identifier() ) ) {
            // We're not going to gossip to ourselves.
            return;
        }
        nodes.add( new GossipNode( ibis ) );
    }

    synchronized void remove( IbisIdentifier ibis )
    {
        extractGossipNode( ibis );
    }

    /** Given an ibis identifier, remove and return any GossipNode for this
     * ibis from the list.
     * @param ibis The ibis to extract.
     * @return The GossipNode of the ibis, or <code>null</code> if there
     *      wasn't any.
     */
    private GossipNode extractGossipNode( IbisIdentifier ibis )
    {
        for( GossipNode m: nodes ) {
            if( ibis.equals( m.ibis ) ) {
                nodes.remove( m );
                return m;
            }
        }
        return null;
    }

    /** Registers that we had a recent update from this ibis (so that we
     * don't have to bother to ask for it ourselves).
     * @param source The ibis for which we had a recent update.
     */
    synchronized void hadRecentUpdate( IbisIdentifier source )
    {
        GossipNode node = extractGossipNode( source );
        if( node == null ) {
            node = new GossipNode( source );
        }
        node.nextUpdateMoment = System.currentTimeMillis() + node.updateInterval;
        nodes.add( node );
    }

    /** Tells the administration that we would urgently like to see
     * an update from the given source.
     * @param source The ibis for which we want an urgent update.
     */
    synchronized void needsUrgentUpdate( IbisIdentifier source )
    {
        GossipNode node = extractGossipNode( source );
        if( node == null ){
            node = new GossipNode( source );
        }
        node.nextUpdateMoment = Math.min( node.nextUpdateMoment, System.currentTimeMillis() );
        nodes.add( node );
    }
}
