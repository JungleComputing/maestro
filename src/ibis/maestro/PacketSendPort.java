package ibis.maestro;

import java.io.IOException;

import ibis.ipl.Ibis;
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
public class PacketSendPort<T> {
    private static final PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.CONNECTION_ONE_TO_ONE );
    private SendPort port;

    /**
     * Constructs a new PacketSendPort.
     * @param ibis The ibis the port will belong to.
     * @param name The name of the port.
     * @throws IOException Thrown if there is an error in the setup.
     */
    PacketSendPort( Ibis ibis, String name ) throws IOException{
        port = ibis.createSendPort(portType, name );
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
}
