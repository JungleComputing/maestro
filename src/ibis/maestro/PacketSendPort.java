package ibis.maestro;

import java.io.IOException;
import java.io.Serializable;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

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
    private static boolean USE_DISCONNECT = false;
    private final SendPort globalport;

    PacketSendPort( Ibis ibis ) throws IOException
    {
	this.ibis = ibis;
	globalport = ibis.createSendPort( portType );
    }

    /**
     * Sends the given data to the given port.
     * @param data The data to send.
     * @param receiver The port to send it to.
     * @param timeout The timeout of the transmission.
     * @return The length of the transmitted data.
     * @throws IOException Thrown if there is a communication error.
     */
    public long send( T data, ReceivePortIdentifier receiver, int timeout ) throws IOException
    {
	long len;

	long startTime = System.nanoTime();
	long setupTime;
	if( USE_DISCONNECT ) {
	    globalport.connect( receiver, timeout, true  );
	    WriteMessage msg = globalport.newMessage();
	    setupTime = System.nanoTime();
	    msg.writeObject( data );
	    len = msg.finish();
	    globalport.disconnect( receiver );
	}
	else {
	    SendPort port = ibis.createSendPort( portType );
	    port.connect( receiver, timeout, true );
	    WriteMessage msg = port.newMessage();
	    setupTime = System.nanoTime();
	    msg.writeObject( data );
	    len = msg.finish();
	    port.close();
	}
	long stopTime = System.nanoTime();
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
    public long send( T data, IbisIdentifier receiver, String portname, int timeout ) throws IOException
    {
	long len;

	long startTime = System.nanoTime();
	long setupTime;
	if( USE_DISCONNECT ) {
	    ReceivePortIdentifier rp = globalport.connect( receiver, portname, timeout, true );
	    WriteMessage msg = globalport.newMessage();
	    setupTime = System.nanoTime();
	    msg.writeObject( data );
	    len = msg.finish();
	    globalport.disconnect(rp);
	}
	else {
	    SendPort port = ibis.createSendPort(portType);
	    port.connect( receiver, portname, timeout, true );
	    WriteMessage msg = port.newMessage();
	    setupTime = System.nanoTime();
	    msg.writeObject( data );
	    len = msg.finish();
	    port.close();
	}
	long stopTime = System.nanoTime();
	if( Settings.traceSends ) {
	    System.out.println( "Sent " + len + " bytes in " + Service.formatNanoseconds(stopTime-setupTime) + "; setup time " + Service.formatNanoseconds(setupTime-startTime) );
	}
	return len;
    }

}
