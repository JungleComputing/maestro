package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.Location;

import java.util.PriorityQueue;

class UnregisteredNodeList
{
    static final class UnregisteredNodeInfo implements Comparable<UnregisteredNodeInfo>
    {
        final IbisIdentifier ibis;
        private boolean reply;
        final boolean local;
        private int tries = 0;
        final NodeIdentifier ourIdentifierForNode;

        UnregisteredNodeInfo( IbisIdentifier ibis, NodeIdentifier ourIdentifierForNode, boolean reply, boolean local )
        {
            this.ibis = ibis;
            this.ourIdentifierForNode = ourIdentifierForNode;
            this.reply = reply;
            this.local = local;
        }
        
        /**
         * Returns a string representation of this registration data. (Overrides method in superclass.)
         * @return The string representation.
         */
        @Override
        public String toString()
        {
            return "registration to " + ibis + " (reply=" + reply + " local=" + local + " tries=" + tries + ")";
        }
        
        synchronized int incrementTries()
        {
            return tries++;
        }

        boolean getReply()
        {
            return reply;
        }
        
        private static int rankIbisIdentifiers( IbisIdentifier local, IbisIdentifier a, IbisIdentifier b )
        {
            if( local == null ) {
                // No way to compare if we don't know what our local ibis is.
                return 0;
            }
            Location la = a.location();
            Location lb = b.location();
            Location home = local.location();
            int na = la.numberOfMatchingLevels( home );
            int nb = lb.numberOfMatchingLevels( home );
            if( na>nb ) {
                return -1;
            }
            if( na<nb ) {
                return 1;
            }
            // Since the number of matching levels is the same, try to
            // rank on another criterium. Since we don't have access to
            // anything more meaningful, use the difference in hash values of the level 0 string.
            // Although not particularly meaningful, at least the local
            // node will have distance 0, and every node will have a different
            // notion of local.
            int hl = home.getLevel( 0 ).hashCode();
            int ha = la.getLevel( 0 ).hashCode();
            int hb = lb.getLevel( 0 ).hashCode();
            int da = Math.abs( hl-ha );
            int db = Math.abs( hl-hb );
            if( da<db ) {
                return -1;
            }
            if( da>db ) {
                return 1;
            }
            return 0;
        }

        /**
         * Compares this node info with the specified node info for order.
         * Returns a negative integer, zero, or a positive integer as this node info is less than, equal to, or greater than the specified node info.
         * The ordering is such that the highest-priority node is the smallest.
         * 
         * @param other The other node to compare to.
         * @return The comparison result.
         */
        @Override
        public int compareTo( UnregisteredNodeInfo other )
        {
            // The one with the lowest number of tries get priority.
            if( this.tries<other.tries ) {
                return -1;
            }
            if( this.tries>other.tries ) {
                return 1;
            }
            // A reply gets priority.
            if( this.reply && !other.reply ) {
                return -1;
            }
            if( !this.reply && other.reply ) {
                return 1;
            }
            return rankIbisIdentifiers( Globals.localIbis.identifier(), this.ibis, other.ibis );
        }
    }

    PriorityQueue<UnregisteredNodeInfo> list = new PriorityQueue<UnregisteredNodeInfo>();
    
    synchronized UnregisteredNodeInfo removeIfAny()
    {
        UnregisteredNodeInfo info = list.poll();
        if( Settings.traceRegistration && info != null ) {
            Globals.log.reportProgress( "Drawing " + info + " from the registration queue" );
        }
        return info;
    }
    
    private UnregisteredNodeInfo containsIbis( IbisIdentifier theIbis )
    {
        for( UnregisteredNodeInfo info: list ) {
            if( info.ibis.equals( theIbis ) ) {
                return info;
            }
        }
        return null;
    }

    /** FIXME.
     * @param theIbis The unregistered ibis.
     * @param local Is this the local ibis?
     * @param reply Is this registration a reply?
     */
    @SuppressWarnings("synthetic-access")
    synchronized void add( NodeInfo nodeInfo, boolean reply )
    {
        UnregisteredNodeInfo info = containsIbis( nodeInfo.ibis );
        if( info != null ) {
            info.reply = true;
            return;
        }
        info = new UnregisteredNodeInfo( nodeInfo.ibis, nodeInfo.ourIdentifierForNode, reply, nodeInfo.local );
        if( Settings.traceRegistration ) {
            Globals.log.reportProgress( "Adding " + info + " to the registration queue" );
        }
        list.add( info );
    }

    synchronized void add( UnregisteredNodeInfo ni )
    {
        if( Settings.traceRegistration ) {
            Globals.log.reportProgress( "Putting " + ni + " back in the registration queue" );
        }
        list.add( ni );
    }
}
