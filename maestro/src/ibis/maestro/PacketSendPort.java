package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

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
    private long sentBytes = 0;
    private long sendTime = 0;
    private long adminTime = 0;
    private int sentCount = 0;
    private int evictions = 0;
    private Counter localSentCount = new Counter();
    private final CacheInfo cache[] = new CacheInfo[Settings.CONNECTION_CACHE_SIZE];
    private int clockHand = 0;

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
                cacheSlot = null;
            }
        }
    }

    /** One entry in the connection cache administration. */
    static class CacheInfo {
        int useCount;                  // If >0, port is currently used. Never evict such an entry.
        boolean recentlyUsed;
        SendPort port;

        void close() throws IOException
        {
            if( port != null ) {
                port.close();
                port = null;
            }
            recentlyUsed = false;
        }
    }

    @SuppressWarnings("synthetic-access")
    PacketSendPort( Node node, IbisIdentifier localIbis )
    {
        this.node = node;
        destinations.put( localIbis, new DestinationInfo( localIbis, true ) );
    }

    /** Return an empty slot in the cache.
     * Assumes there is a lock on 'this'.
     */
    private int searchCacheEmptySlot()
    {
        for(;;){
            CacheInfo e = cache[clockHand];
            if( e == null || e.port == null ){
                // Prefer empty cache slots, or slots with null ports.
                return clockHand;
            }
            if( e.useCount<=0 ) {
                if( e.recentlyUsed ){
                    // Next round it will not be considered recently used,
                    // unless it is used. For now don't consider it an
                    // empty slot.
                    e.recentlyUsed = false;
                }
                else {
                    return clockHand;
                }
            }
            clockHand++;
            if( clockHand>=cache.length ){
                clockHand = 0;
            }
        }
    }

    /**
     * Create a cache slot for the given connection. If necessary evict the
     * old entry.
     * @throws IOException 
     */
    @SuppressWarnings("synthetic-access")
    private void ensureOpenDestination( DestinationInfo newDestination, int timeout ) throws IOException
    {
        if( newDestination.local || newDestination.cacheSlot != null ){
            return;
        }
        long tStart = System.nanoTime();
        int ix = searchCacheEmptySlot();

        CacheInfo cacheInfo = cache[ix];
        if( cacheInfo == null ){
            // An unused cache slot. Start to use it.
            cacheInfo = cache[ix] = new CacheInfo();
        }
        else {
            // Somebody was using this cache slot. Evict him.
            cacheInfo.close();
            evictions++;
        }
        newDestination.cacheSlot = cacheInfo;
        SendPort port = Globals.localIbis.createSendPort( portType );
        port.connect( newDestination.ibisIdentifier, Globals.receivePortName, timeout, true );
        long tEnd = System.nanoTime();
        adminTime += (tEnd-tStart);
        cacheInfo.port = port;
        cacheInfo.useCount = 0;  // Should be 0, but paranoia doesn't hurt here.
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
     * @param timeout The timeout of the transmission.
     * @return The length of the transmitted data.
     * @throws IOException Thrown if there is a communication error.
     */
    @SuppressWarnings("synthetic-access")
    private boolean send( IbisIdentifier theIbis, Message message, int timeout ) throws IOException
    {
        long len;
        boolean ok = true;
        DestinationInfo info = destinations.get( theIbis );

        if( info == null ) {
            info = new DestinationInfo( theIbis, false );  // We know the local node has registered itself.
            destinations.put( theIbis, info );
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

            try {
                SendPort port;
                long startTime;
                final CacheInfo cacheInfo;

                synchronized( this ) {
                    ensureOpenDestination( info, timeout );
                    startTime = System.nanoTime();
                    cacheInfo = info.cacheSlot;
                    cacheInfo.recentlyUsed = true;
                    cacheInfo.useCount++;
                    port = cacheInfo.port;
                }
                WriteMessage msg = port.newMessage();
                msg.writeObject( message );
                len = msg.finish();
                if( len<0 ) {
                    ok = false;
                    len = 0;
                }
                synchronized( this ) {
                    cacheInfo.useCount--;
                    sentBytes += len;
                    sentCount++;
                    t = System.nanoTime()-startTime;
                    sendTime += t;
                }
            }
            catch ( IOException x ) {
                info.close();
                throw x;
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
        Comparator<? super DestinationInfo> comparator = new DestinationInfo.InfoComparator();
        Arrays.sort( l, 0, sz, comparator );
        for( int ix=0; ix<sz; ix++ ) {
            DestinationInfo i = l[ix];

            i.printStatistics( s );
        }
    }

    /**
     * Sends the given data to the given port.
     * @param msg The data to send.
     * @param destination The port to send it to.
     * @param timeout The timeout of the transmission.
     * @return <code>true</code> if the message could be sent.
     */
    @SuppressWarnings("synthetic-access")
    boolean tryToSend( IbisIdentifier id, Message msg, int timeout )
    {
        boolean ok = false;
        try {
            ok = send( id, msg, timeout );
        }
        catch (IOException e) {
            DestinationInfo info = destinations.get( id );
            node.setSuspect( info.ibisIdentifier );
            Globals.log.reportError( "Cannot send a " + msg.getClass() + " message to master " + id );
            e.printStackTrace( Globals.log.getPrintStream() );
        }
        return ok;
    }
}
