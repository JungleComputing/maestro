package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author Kees van Reeuwijk
 *
 * A port that communicates in whole objects.
 *
 * @param <T> The type of data that will be sent over this port.
 */
public class PacketSendPort<T extends Serializable> {
    static final PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT, PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_EXPLICIT );
    private final Ibis ibis;
    private long sentBytes = 0;
    private long sendTime = 0;
    private long adminTime = 0;
    private int sentCount = 0;
    private int evictions = 0;
    private final CacheInfo cache[] = new CacheInfo[Settings.CONNECTION_CACHE_SIZE];
    int clockHand = 0;

    /** The list of known destinations.
     * Register a destination before trying to send to it.
     */
    private ArrayList<DestinationInfo> destinations = new ArrayList<DestinationInfo>();

    /** One entry in the list of destinations. */
    static class DestinationInfo {
        CacheInfo cacheSlot;
        int sentCount = 0;
        int sentBytes = 0;
        final ReceivePortIdentifier portIdentifier;

        public DestinationInfo( ReceivePortIdentifier portIdentifier ){
            this.portIdentifier = portIdentifier;
        }
        
        
    }

    /** One entry in the connection cache administration. */
    static class CacheInfo {
        DestinationInfo destination;
        boolean recentlyUsed;
        SendPort port;
    }

    PacketSendPort( Ibis ibis )
    {
        this.ibis = ibis;
    }

    /** Return an empty slot in the cache. */
    private int searchEmptySlot()
    {
        for(;;){
            CacheInfo e = cache[clockHand];
            if( e == null ){
                return clockHand;
            }
            if( e.recentlyUsed ){
                e.recentlyUsed = false;
            }
            else {
                return clockHand;
            }
            clockHand++;
            if( clockHand>=cache.length ){
                clockHand = 0;
            }
        }
    }

    /**
     * Create a cache slot for the given connection. If necessary evict the old user.
     * @return the cache slot that was reserved.
     * @throws IOException 
     */
    private void ensureOpenDestination( DestinationInfo newDestination, int timeout ) throws IOException
    {
        if( newDestination.cacheSlot != null ){
            return;
        }
        int ix = searchEmptySlot();
        
        CacheInfo e = cache[ix];
        if( e == null ){
            // An unused cache slot. Start to use it.
            e = cache[ix] = new CacheInfo();
        }
        else {
            // Somebody was using this cache slot. Evict him.
            e.port.close();
            e.destination.cacheSlot = null;
            evictions++;
        }
        e.destination = newDestination;
        newDestination.cacheSlot = e;
        long tStart = System.nanoTime();
        SendPort port = ibis.createSendPort( portType );
        port.connect( newDestination.portIdentifier, timeout, true );
        long tEnd = System.nanoTime();
        adminTime += (tEnd-tStart);
        e.port = port;
    }

    /**
     * Given a receive port, registers it with this packet send port, and returns an identifier of the port.
     * @param port The port to register.
     * @param identifier The identifier we will use for it.
     */
    void registerDestination( ReceivePortIdentifier port, int identifier )
    {
        while( destinations.size()<=identifier ) {
            destinations.add( null );
        }
        if( destinations.get( identifier ) != null ) {
            System.err.println( "Internal error: duplicate registration for sendport ID " + identifier + ": old=" + destinations.get( identifier ) + "; new=" + port );
        }
        destinations.set( identifier, new DestinationInfo( port ) );
    }

    /**
     * Sends the given data to the given port.
     * @param destination The port to send it to.
     * @param data The data to send.
     * @param timeout The timeout of the transmission.
     * @return The length of the transmitted data.
     * @throws IOException Thrown if there is a communication error.
     */
    private synchronized long send( int destination, T data, int timeout ) throws IOException
    {
        long len;

        DestinationInfo info = destinations.get( destination );
        ensureOpenDestination( info, timeout );
        long startTime = System.nanoTime();
        final CacheInfo cacheInfo = info.cacheSlot;
        WriteMessage msg = cacheInfo.port.newMessage();
        msg.writeObject( data );
        len = msg.finish();
        cacheInfo.recentlyUsed = true;
        long stopTime = System.nanoTime();
        sendTime += (stopTime-startTime);
        sentBytes += len;
        sentCount++;
        info.sentBytes += len;
        info.sentCount++;
        if( Settings.traceSends ) {
            System.out.println( "Sent " + len + " bytes in " + Service.formatNanoseconds(stopTime-startTime) );
        }
        return len;
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
    public synchronized long send( IbisIdentifier receiver, String portname, T data, int timeout ) throws IOException
    {
        long len;

        long startTime = System.nanoTime();
        long setupTime;
        SendPort port = ibis.createSendPort(portType);
        port.connect( receiver, portname, timeout, true );
        WriteMessage msg = port.newMessage();
        setupTime = System.nanoTime();
        msg.writeObject( data );
        len = msg.finish();
        port.close();
        long stopTime = System.nanoTime();
        if( Settings.traceSends ) {
            System.out.println( "Sent " + len + " bytes in " + Service.formatNanoseconds(stopTime-setupTime) + "; setup time " + Service.formatNanoseconds(setupTime-startTime) );
        }
        adminTime += (setupTime-startTime);
        sendTime += (stopTime-setupTime);
        sentBytes += len;
        sentCount++;
        return len;
    }

    /** Given the name of this port, prints some statistics about this port.
     * 
     * @param portname The name of the port.
     */
    public synchronized void printStats( String portname )
    {
        System.out.println( portname + ": sent " + sentBytes + " bytes in " + sentCount + " messages; " + evictions + " evictions" );
        if( sentCount>0 ) {
            System.out.println( portname + ": total send time  " + Service.formatNanoseconds( sendTime ) + "; " + Service.formatNanoseconds( sendTime/sentCount ) + " per message" );
            System.out.println( portname + ": total setup time " + Service.formatNanoseconds( adminTime ) + "; " + Service.formatNanoseconds( adminTime/sentCount ) + " per message" );
        }
    }

    /** 
     * Tries to send a message to the given ibis and port name.
     * @param ibis2 The ibis to send the message to.
     * @param portName The port to send  the message to.
     * @param msg The message to send.
     * @param timeout The timeout on the message.
     * @return The number of transmitted bytes, or 0 if the message could not be sent.
     */
    public long tryToSend(IbisIdentifier ibis2, String portName, T msg, int timeout) {
        long sz = 0;
        try {
            sz = send( ibis2, portName, msg, timeout );
        } catch (IOException e) {
            Globals.log.reportError( "Cannot send a " + msg.getClass() + " message to ibis " + ibis2 );
            e.printStackTrace( Globals.log.getPrintStream() );
        }
        return sz;
    }


    /**
     * Sends the given data to the given port.
     * @param msg The data to send.
     * @param destination The port to send it to.
     * @param timeout The timeout of the transmission.
     * @return The length of the transmitted data, or 0 if nothing could be transmitted.
     */
    public long tryToSend( int destination, T msg, int timeout ) {
        long sz = 0;
        try {
            sz = send( destination, msg, timeout );
        } catch (IOException e) {
            Globals.log.reportError( "Cannot send a " + msg.getClass() + " message to master " + destination );
            e.printStackTrace( Globals.log.getPrintStream() );
        }
        return sz;
    }

}
