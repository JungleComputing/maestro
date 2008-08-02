package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

/**
 * A port that communicates in entire objects.
 *
 * @author Kees van Reeuwijk
 *
 */
class PacketSendPort {
    static final PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT, PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_EXPLICIT );
    private final Ibis ibis;
    private final Node node;  // The node this runs on.
    private long sentBytes = 0;
    private long sendTime = 0;
    private long adminTime = 0;
    private int sentCount = 0;
    private int evictions = 0;
    private long uncachedSentBytes = 0;
    private long uncachedSendTime = 0;
    private long uncachedAdminTime = 0;
    private long uncachedSentCount = 0;
    private Counter localSentCount = new Counter();
    private final CacheInfo cache[] = new CacheInfo[Settings.CONNECTION_CACHE_SIZE];
    private final HashMap<ReceivePortIdentifier, Integer> PortToIdMap = new HashMap<ReceivePortIdentifier, Integer>();
    private PacketReceiveListener localListener = null;
    private int clockHand = 0;

    /** The list of known destinations.
     * Register a destination before trying to send to it.
     */
    private ArrayList<DestinationInfo> destinations = new ArrayList<DestinationInfo>();

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
        private final ReceivePortIdentifier portIdentifier;
        boolean local;

        /** Create a new destination info entry.
         * @param portIdentifier The destination port.
         * @param local True iff this destination represents the local master or worker.
         */
        private DestinationInfo( ReceivePortIdentifier portIdentifier, boolean local ){
            this.portIdentifier = portIdentifier;
            this.local = local;
        }

        /** Print statistics for this destination. */
        private synchronized void printStats( PrintStream s )
        {
            char dest = local?'L':'R'; 
            s.format( " %c %5d messages %7d bytes; port %s\n", dest, sentCount, sentBytes, portIdentifier.toString() );
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
        DestinationInfo destination;
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
            destination = null;
        }
    }

    PacketSendPort( Ibis ibis, Node node )
    {
        this.ibis = ibis;
        this.node = node;
    }

    synchronized void setLocalListener( PacketReceiveListener localListener )
    {
        if( this.localListener != null ) {
            System.err.println( "Cannot change the local listener" );
            return;
        }
        this.localListener = localListener;
    }

    /** Return an empty slot in the cache.
     * Assumes there is a lock on 'this'.
     */
    private int searchEmptySlot()
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
        int ix = searchEmptySlot();

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
        cacheInfo.destination = newDestination;
        newDestination.cacheSlot = cacheInfo;
        SendPort port = ibis.createSendPort( portType );
        port.connect( newDestination.portIdentifier, timeout, true );
        long tEnd = System.nanoTime();
        adminTime += (tEnd-tStart);
        cacheInfo.port = port;
        cacheInfo.useCount = 0;  // Should be 0, but paranoia doesn't hurt here.
    }

    /**
     * Given a receive port, registers it with this packet send port, and returns an identifier of the port.
     * @param port The port to register.
     * @param identifier The identifier we will use for it.
     */
    @SuppressWarnings("synthetic-access")
    synchronized void registerDestination( ReceivePortIdentifier port, int identifier )
    {
        if( Settings.traceRegistration ) {
            Globals.log.reportProgress( "PacketSendPort(): id=" + identifier + "->" + port );
        }
        while( destinations.size()<=identifier ) {
            destinations.add( null );
        }
        PortToIdMap.put( port, identifier );
        DestinationInfo destinationInfo = destinations.get( identifier );
        if( destinationInfo != null ) {
            if( !port.equals( destinationInfo.portIdentifier ) ) {
                System.err.println( "Internal error: two different registrations for sendport ID " + identifier + ": old=" + destinationInfo + "; new=" + port );
            }
        }
        boolean local = localListener.hasReceivePort( port );
        destinations.set( identifier, new DestinationInfo( port, local ) );
    }

    /**
     * Sends the given data to the given port.
     * @param destination The port to send it to.
     * @param message The data to send.
     * @param timeout The timeout of the transmission.
     * @return The length of the transmitted data.
     * @throws IOException Thrown if there is a communication error.
     */
    private boolean send( int destination, Message message, int timeout ) throws IOException
    {
        long len;
        boolean ok = true;
        DestinationInfo info = destinations.get( destination );

        info.incrementSentCount();
        if( info.local ) {
            // This is the local destination. Use the back door to get
            // the info to the destination.
            message.arrivalMoment = System.nanoTime();
            localListener.messageReceived( message );
            len = 0;
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
                synchronized( port ) {
                    WriteMessage msg = port.newMessage();
                    msg.writeObject( message );
                    len = msg.finish();
                    if( len<0 ) {
                        ok = false;
                        len = 0;
                    }
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

    /**
     * Sends the given data to the port with the given name on the given ibis.
     * @param receiver The port to send it to.
     * @param portname The name of the port to send to.
     * @param data The data to send.
     * @param timeout The timeout on the port.
     * @return The length of the transmitted data.
     * @throws IOException Thrown if there is a communication error.
     */
    private long send( IbisIdentifier receiver, String portname, Message data, int timeout ) throws IOException
    {
        long len;

        synchronized( this ) {
            long startTime = System.nanoTime();
            SendPort port = ibis.createSendPort( portType );
            port.connect( receiver, portname, timeout, true );
            long setupTime = System.nanoTime();
            WriteMessage msg = port.newMessage();
            msg.writeObject( data );
            len = msg.finish();
            port.close();
            long stopTime = System.nanoTime();
            if( Settings.traceSends ) {
                System.out.println( "Sent " + len + " bytes in " + Service.formatNanoseconds(stopTime-setupTime) + "; setup time " + Service.formatNanoseconds(setupTime-startTime) + ": " + data );
            }
            uncachedAdminTime += (setupTime-startTime);
            uncachedSendTime += (stopTime-setupTime);
            uncachedSentBytes += len;
            uncachedSentCount++;
        }
        return len;
    }

    /** Given the name of this port, prints some statistics about this port.
     * 
     * @param portname The name of the port.
     */
    @SuppressWarnings("synthetic-access")
    synchronized void printStatistics( PrintStream s, String portname )
    {
        s.println( portname + ": sent " + sentBytes + " bytes in " + sentCount + " remote messages; " + localSentCount.get() + " local sends; "+ evictions + " evictions" );
        if( sentCount>0 ) {
            s.println( portname + ": total send time  " + Service.formatNanoseconds( sendTime ) + "; " + Service.formatNanoseconds( sendTime/sentCount ) + " per message" );
            s.println( portname + ": total setup time " + Service.formatNanoseconds( adminTime ) + "; " + Service.formatNanoseconds( adminTime/sentCount ) + " per message" );
        }
        s.println( portname + ": sent " + uncachedSentBytes + " bytes in " + uncachedSentCount + " uncached remote messages" );
        if( uncachedSentCount>0 ) {
            s.println( portname + ": total uncached send time  " + Service.formatNanoseconds( uncachedSendTime ) + "; " + Service.formatNanoseconds( uncachedSendTime/uncachedSentCount ) + " per message" );
            s.println( portname + ": total uncached setup time " + Service.formatNanoseconds( uncachedAdminTime ) + "; " + Service.formatNanoseconds( uncachedAdminTime/uncachedSentCount ) + " per message" );
        }
        DestinationInfo l[] = new DestinationInfo[destinations.size()];
        int sz = 0;
        for( DestinationInfo i: destinations ) {
            if( i != null ) {
                l[sz++] = i;
            }
        }
        Comparator<? super DestinationInfo> comparator = new DestinationInfo.InfoComparator();
        Arrays.sort( l, 0, sz, comparator );
        for( int ix=0; ix<sz; ix++ ) {
            DestinationInfo i = l[ix];

            i.printStats( s );
        }
    }

    /** 
     * Tries to send a message to the given ibis and port name.
     * @param theIbis The ibis to send the message to.
     * @param portName The port to send  the message to.
     * @param msg The message to send.
     * @param timeout The timeout on the message.
     * @return The number of transmitted bytes, or -1 if the message could not be sent.
     */
    long tryToSend( IbisIdentifier theIbis, String portName, Message msg, int timeout )
    {
        long sz = -1;
        try {
            sz = send( theIbis, portName, msg, timeout );
        } catch (IOException e) {
            node.setSuspect( theIbis );
            Globals.log.reportError( "Cannot send a " + msg.getClass() + " message to ibis " + theIbis );
            e.printStackTrace( Globals.log.getPrintStream() );
        }
        return sz;
    }


    /**
     * Sends the given data to the given port.
     * @param msg The data to send.
     * @param destination The port to send it to.
     * @param timeout The timeout of the transmission.
     * @return <code>true</code> if the message could be sent.
     */
    @SuppressWarnings("synthetic-access")
    boolean tryToSend( NodeIdentifier id, Message msg, int timeout )
    {
        int destination = id.value;
        boolean ok = false;
        try {
            ok = send( destination, msg, timeout );
        }
        catch (IOException e) {
            DestinationInfo info = destinations.get( destination );
            node.setSuspect( info.portIdentifier.ibisIdentifier() );
            Globals.log.reportError( "Cannot send a " + msg.getClass() + " message to master " + destination );
            e.printStackTrace( Globals.log.getPrintStream() );
        }
        return ok;
    }

    /**
     * Tries to send a message to the given receive port.
     * @param port The port to send the message to.
     * @param data The message to send.
     * @param timeout The timeout value to use.
     * @return <code>true</code> if the message could be sent.
     */
    boolean tryToSend( ReceivePortIdentifier port, Message data, int timeout )
    {
        boolean ok = false;
        try {
            Integer destination = PortToIdMap.get( port );
            if( destination != null ) {
                // We have this one registered, use that port.
                return send( destination, data, timeout );
            }
            synchronized( this ) {
                // We don't have information about this destination,
                // just send it.
                long tStart = System.nanoTime();
                SendPort sendPort = ibis.createSendPort( portType );
                sendPort.connect( port, timeout, true );
                long tEnd = System.nanoTime();
                adminTime += (tEnd-tStart);
                WriteMessage msg = sendPort.newMessage();
                long setupTime = System.nanoTime();
                msg.writeObject( data );
                long len = msg.finish();
                sendPort.close();
                long stopTime = System.nanoTime();
                uncachedAdminTime += (setupTime-tStart);
                uncachedSendTime += (stopTime-setupTime);
                uncachedSentBytes += len;
                uncachedSentCount++;
                if( Settings.traceSends ) {
                    System.out.println( "Sent " + len + " bytes in " + Service.formatNanoseconds(stopTime-setupTime) + "; setup time " + Service.formatNanoseconds(setupTime-tStart) );
                }
            }
        } catch (IOException e) {
            Globals.log.reportError( "Cannot send a " + data.getClass() + " message to master " + port );
            e.printStackTrace( Globals.log.getPrintStream() );
        }
        return ok;
    }
}
