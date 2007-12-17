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
    private SendPort port;

    /**
     * Constructs a new PacketSendPort.
     * @param ibis The ibis the port will belong to.
     * @throws IOException Thrown if there is an error in the setup.
     */
    PacketSendPort( Ibis ibis ) throws IOException{
        port = ibis.createSendPort(portType );
    }

    /**
     * Sends the given data to the given port.
     * @param data The data to send.
     * @param receiver The port to send it to.
     * @throws IOException Thrown if there is a communication error.
     */
    public void send( T data, ReceivePortIdentifier receiver ) throws IOException {
        port.connect(receiver);
        WriteMessage msg = port.newMessage();
        msg.writeObject( data );
        msg.finish();
        port.close();
    }

    /**
     * Sends the given data to the port with the given name on the given ibis.
     * @param data The data to send.
     * @param receiver The port to send it to.
     * @throws IOException Thrown if there is a communication error.
     */
    public void send( T data, IbisIdentifier receiver, String portname ) throws IOException {
        port.connect( receiver, portname );
        WriteMessage msg = port.newMessage();
        msg.writeObject( data );
        msg.finish();
        port.close();
    }

}
