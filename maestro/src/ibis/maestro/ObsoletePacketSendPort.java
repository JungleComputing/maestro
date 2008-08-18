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
class ObsoletePacketSendPort {
    static final PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT, PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_EXPLICIT );
    private final Node node;  // The node this runs on.
    private long sentBytes = 0;
    private long sendTime = 0;
    private long adminTime = 0;
    private int sentCount = 0;
    private int evictions = 0;
    private Counter localSentCount = new Counter();
    private final CacheInfo cache[] = new CacheInfo[Settings.CONNECTION_CACHE_SIZE];
    private int clockHand = 0;   // The next cache slot we might want to evict, traversing the list circularly

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
            s.format( " %c %5d messages %5s   node %s\n", dest, sentCount, Utils.formatByteCount( sentBytes ), ibisIdentifier.toString() );
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
    ObsoletePacketSendPort( Node node, IbisIdentifier localIbis )
    {
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

    /** Return an empty slot in the cache.
     * Assumes there is a lock on 'this'.
     */
    private int searchEmptyCacheSlot()
    {
        for(;;){
            CacheInfo e = cache[clockHand];
            if( e == null ){
                return clockHand;
            }
            if( e.useCount<=0 ) {
                if( e.port == null ){
                    // Prefer empty cache slots, or slots with null ports.
                    return clockHand;
                }
                // Only consider slots that are not in use.
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
     */
    @SuppressWarnings("synthetic-access")
    private CacheInfo getAndReserveCacheInfo( DestinationInfo newDestination, int timeout )
    {
        CacheInfo cacheInfo;
        long tStart;

        synchronized( this ){
            // We need a lock as long as we don't have a cache slot with
            // a non-zero use count.
            if( newDestination.local || newDestination.cacheSlot != null ){
                cacheInfo = newDestination.cacheSlot;
                cacheInfo.recentlyUsed = true;
                cacheInfo.useCount++;
                return cacheInfo;
            }
            tStart = System.nanoTime();
            int ix = searchEmptyCacheSlot();

            cacheInfo = cache[ix];
            if( cacheInfo == null ){
                // An unused cache slot. Start to use it.
                cacheInfo = cache[ix] = new CacheInfo();
            }
            else {
                // Somebody was using this cache slot. Evict him.
                try {
                    cacheInfo.close();
                }
                catch ( IOException x ){
                    // The previous connection could not be closed.
                    // Too bad, but there is nothing we can do about it.
                }
                evictions++;
            }
            cacheInfo.recentlyUsed = true;
            cacheInfo.useCount = 1;
            newDestination.cacheSlot = cacheInfo;
            cacheInfo.owner = newDestination;
        }
        try {
            SendPort port = Globals.localIbis.createSendPort( portType );
            port.connect( newDestination.ibisIdentifier, Globals.receivePortName, timeout, true );
            cacheInfo.port = port;
        }
        catch( IOException x ){
            synchronized( this ){
                // Release our hold on this cache slot; we're
                // not going to use it.
                cacheInfo.useCount--;
            }
            return null;
        }
        long tEnd = System.nanoTime();
        synchronized( this ){
            adminTime += (tEnd-tStart);
        }
        return cacheInfo;
    }

    /**
     * Sends the given data to the given port.
     * @param theIbis The port to send it to.
     * @param message The data to send.
     * @param timeout The timeout of the transmission.
     * @return <code>true</code> if we managed to send the data.
     * @throws IOException Thrown if there is a communication error.
     */
    @SuppressWarnings("synthetic-access")
    private boolean send( IbisIdentifier theIbis, Message message, int timeout ) throws IOException
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

            try {
                final CacheInfo cacheInfo = getAndReserveCacheInfo( info, timeout );
                if( cacheInfo == null ){
                    // Couldn't open a connection to the destination.
                    return false;
                }
                SendPort port = cacheInfo.port;
                long startTime = System.nanoTime();
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
                System.out.println( "Sent " + len + " bytes in " + Utils.formatNanoseconds( t ) + ": " + message );
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
        s.println( portname + ": sent " + Utils.formatByteCount( sentBytes ) + " in " + sentCount + " remote messages; " + localSentCount.get() + " local sends; "+ evictions + " evictions" );
        if( sentCount>0 ) {
            s.println( portname + ": total send time  " + Utils.formatNanoseconds( sendTime ) + "; " + Utils.formatNanoseconds( sendTime/sentCount ) + " per message" );
            s.println( portname + ": total setup time " + Utils.formatNanoseconds( adminTime ) + "; " + Utils.formatNanoseconds( adminTime/sentCount ) + " per message" );
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
            node.setSuspect( id );
            Globals.log.reportError( "Cannot send a " + msg.getClass() + " message to master " + id );
            e.printStackTrace( Globals.log.getPrintStream() );
        }
        return ok;
    }
}
