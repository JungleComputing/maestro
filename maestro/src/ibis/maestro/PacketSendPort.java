package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.SendPort;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * A port that communicates in entire objects.
 *
 * @author Kees van Reeuwijk
 *
 */
class PacketSendPort {
    static final PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT, PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_EXPLICIT );
    private final Node node;  // The node this runs on.
    private final ConnectionCache connectionCache;
    private long sentBytes = 0;
    private long sendTime = 0;
    private long adminTime = 0;
    private int sentCount = 0;
    private int evictions = 0;
    private Counter localSentCount = new Counter();

    /** The list of known destinations.
     * Register a destination before trying to send to it.
     */
    private HashMap<IbisIdentifier,DestinationInfo> destinations = new HashMap<IbisIdentifier,DestinationInfo>();

    /** One entry in the list of destinations. */
    private static final class DestinationInfo {
        private static final class InfoComparator implements Comparator<DestinationInfo>, Serializable {
            private static final long serialVersionUID = 9141273343902181193L;
            /**
             * Compares the two given destination info class instances. This comparator ensures
             * that the class instances are sorted by decreasing sentCount. To provide a
             * stable sort when the sentCount is the same (can happen for corner cases),
             * it also compares for other fields.
             * @param a The first class instance to compare.
             * @param b The second class instance to compare.
             * @return The comparison result.
             */
            @SuppressWarnings("synthetic-access")
            @Override
            public int compare(DestinationInfo a, DestinationInfo b) {
                if( a.sentCount<b.sentCount ){
                    return 1;
                }
                if( a.sentCount>b.sentCount ){
                    return -1;
                }
                if( a.sentBytes<b.sentBytes ){
                    return 1;
                }
                if( a.sentBytes>b.sentBytes ){
                    return -1;
                }
                if( !a.local && b.local ){
                    return 1;
                }
                if( a.local && !b.local ){
                    return -1;
                }
                return 0;
            }

        }

        CacheInfo cacheSlot;
        private int sentCount = 0;
        private long sentBytes = 0;
        private final IbisIdentifier ibisIdentifier;
        boolean local;

        /** Create a new destination info entry.
         * @param ibisIdentifier The destination ibis.
         * @param local True iff this destination represents the local node.
         */
        private DestinationInfo( IbisIdentifier ibisIdentifier, boolean local )
        {
            this.ibisIdentifier = ibisIdentifier;
            this.local = local;
        }

        /** Print statistics for this destination. */
        private synchronized void printStatistics( PrintStream s )
        {
            char dest = local?'L':'R'; 
            s.format( " %c %5d messages %5s   node %s\n", dest, sentCount, Service.formatByteCount( sentBytes ), ibisIdentifier.toString() );
        }

        synchronized void incrementSentCount()
        {
            sentCount++;
        }

        synchronized void addSentBytes( long val )
        {
            sentBytes += val;
        }

        /** Close this port.
         * @throws IOException 
         * 
         */
        synchronized void close() throws IOException
        {
            if( cacheSlot != null ) {
                cacheSlot.close();
            }
        }
    }

    /** One entry in the connection cache administration. */
    static class CacheInfo {
        int useCount;                  // If >0, port is currently used. Never evict such an entry.
        boolean recentlyUsed;
        DestinationInfo owner;
        SendPort port;

        void close() throws IOException
        {
            if( port != null ) {
                port.close();
                port = null;
            }
            owner.cacheSlot = null;
            owner = null;
            recentlyUsed = false;
        }
    }

    @SuppressWarnings("synthetic-access")
    PacketSendPort( Node node, IbisIdentifier localIbis )
    {
        connectionCache = new ConnectionCache( node );
        this.node = node;
        destinations.put( localIbis, new DestinationInfo( localIbis, true ) );
    }

    /**
     * Given a receive port, registers it with this packet send port, and returns an identifier of the port.
     * @param theIbis The port to register.
     */
    @SuppressWarnings("synthetic-access")
    synchronized void registerDestination( IbisIdentifier theIbis )
    {
        DestinationInfo destinationInfo = destinations.get( theIbis );
        if( destinationInfo != null ) {
            // Already registered. Don't worry about the duplication.
            return;
        }
        destinations.put( theIbis, new DestinationInfo( theIbis, false ) );
    }


    /**
     * Sends the given data to the given port.
     * @param theIbis The port to send it to.
     * @param message The data to send.
     * @return <code>true</code> if we managed to send the data.
     */
    @SuppressWarnings("synthetic-access")
    boolean send( IbisIdentifier theIbis, Message message )
    {
        long len;
        boolean ok = true;
        DestinationInfo info;
        synchronized( this ) {
            info = destinations.get( theIbis );

            if( info == null ) {
        	info = new DestinationInfo( theIbis, false );  // We know the local node has registered itself.
        	destinations.put( theIbis, info );
            }
        }
        info.incrementSentCount();
        if( info.local ) {
            // This is the local destination. Use the back door to get
            // the info to the destination.
            message.arrivalMoment = System.nanoTime();
            node.messageReceived( message );
            len = 0;  // We're not going to compute a size just for the statistics.
            localSentCount.add();
            if( Settings.traceSends ) {
                System.out.println( "Sent local message " + message );
            }
        }
        else {
            long t;
            
            long startTime = System.nanoTime();
            len = connectionCache.sendMessage( theIbis, message );
            if( len<0 ) {
                ok = false;
                len = 0;
            }
            synchronized( this ) {
                sentBytes += len;
                sentCount++;
                t = System.nanoTime()-startTime;
                sendTime += t;
            }
            info.addSentBytes( len );
            if( Settings.traceSends ) {
                System.out.println( "Sent " + len + " bytes in " + Service.formatNanoseconds( t ) + ": " + message );
            }
        }
        return ok;
    }

    /** Given the name of this port, prints some statistics about this port.
     * 
     * @param portname The name of the port.
     */
    @SuppressWarnings("synthetic-access")
    synchronized void printStatistics( PrintStream s, String portname )
    {
        s.println( portname + ": sent " + Service.formatByteCount( sentBytes ) + " in " + sentCount + " remote messages; " + localSentCount.get() + " local sends; "+ evictions + " evictions" );
        if( sentCount>0 ) {
            s.println( portname + ": total send time  " + Service.formatNanoseconds( sendTime ) + "; " + Service.formatNanoseconds( sendTime/sentCount ) + " per message" );
            s.println( portname + ": total setup time " + Service.formatNanoseconds( adminTime ) + "; " + Service.formatNanoseconds( adminTime/sentCount ) + " per message" );
        }
        DestinationInfo l[] = new DestinationInfo[destinations.size()];
        int sz = 0;
	for( Map.Entry<IbisIdentifier, DestinationInfo> entry : destinations.entrySet() ) {
	    DestinationInfo i = entry.getValue();

	    if( i != null ) {
                l[sz++] = i;
            }
        }
	connectionCache.printStatistics( s );
        Comparator<? super DestinationInfo> comparator = new DestinationInfo.InfoComparator();
        Arrays.sort( l, 0, sz, comparator );
        for( int ix=0; ix<sz; ix++ ) {
            DestinationInfo i = l[ix];

            i.printStatistics( s );
        }
    }
}
