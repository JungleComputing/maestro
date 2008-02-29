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

    /** The list of known destinations.
     * Register a destination before trying to send to it.
     */
    private ArrayList<ReceivePortIdentifier> destinations = new ArrayList<ReceivePortIdentifier>();

    PacketSendPort( Ibis ibis )
    {
        this.ibis = ibis;
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
        destinations.set( identifier, port );
    }

    /**
     * Sends the given data to the given port.
     * @param data The data to send.
     * @param destination The port to send it to.
     * @param timeout The timeout of the transmission.
     * @return The length of the transmitted data.
     * @throws IOException Thrown if there is a communication error.
     */
    public synchronized long send( T data, int destination, int timeout ) throws IOException
    {
        long len;

        long startTime = System.nanoTime();
        long setupTime;
        ReceivePortIdentifier receivePort = destinations.get( destination );
        SendPort port = ibis.createSendPort( portType );
        port.connect( receivePort, timeout, true );
        setupTime = System.nanoTime();
        WriteMessage msg = port.newMessage();
        msg.writeObject( data );
        len = msg.finish();
        port.close();
        long stopTime = System.nanoTime();
        adminTime += (setupTime-startTime);
        sendTime += (stopTime-setupTime);
        sentBytes += len;
        sentCount++;
        if( Settings.traceSends ) {
            System.out.println( "Sent " + len + " bytes in " + Service.formatNanoseconds(stopTime-setupTime) + "; setup time " + Service.formatNanoseconds(setupTime-startTime) );
        }
        return len;
    }

    /**
     * Sends the given data to the port with the given name on the given ibis.
     * @param data The data to send.
     * @param receiver The port to send it to.
     * @param portname The name of the port to send to.
     * @param timeout The timeout on the port.
     * @return The length of the transmitted data.
     * @throws IOException Thrown if there is a communication error.
     */
    public synchronized long send( T data, IbisIdentifier receiver, String portname, int timeout ) throws IOException
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
    public void printStats( String portname )
    {
        System.out.println( portname + ": sent " + sentBytes + " bytes in " + sentCount + " messages" );
        if( sentCount>0 ) {
            System.out.println( portname + ": total send time  " + Service.formatNanoseconds( sendTime ) + "; " + Service.formatNanoseconds( sendTime/sentCount ) + " per message" );
            System.out.println( portname + ": total setup time " + Service.formatNanoseconds( adminTime ) + "; " + Service.formatNanoseconds( adminTime/sentCount ) + " per message" );
        }
    }

}
