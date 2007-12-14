package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;

/**
 * @author Kees van Reeuwijk
 *
 * A packet receiving port with an explicit receive() method.
 *
 * @param <T> The type of packets that are received over this port.
 */
public class PacketBlockingReceivePort<T> {
    private static final PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.CONNECTION_MANY_TO_ONE );
    private ReceivePort port;

    /**
     * Constructs a new PacketSendPort.
     * @param ibis The ibis the port will belong to.
     * @param name The name of the port.
     * @throws IOException
     */
    PacketBlockingReceivePort( Ibis ibis, String name ) throws IOException{
        port = ibis.createReceivePort(portType, name );
        port.enableConnections();
    }

    /**
     * Waits for, and then returns a packet.
     * @return A packet.
     * @throws IOException Thrown on an I/O error in the communication system.
     * @throws ClassNotFoundException Thrown if the serialization mechanism encounters an unknown class.
     */
    @SuppressWarnings("unchecked")
    public T receive() throws IOException, ClassNotFoundException {
        ReadMessage msg = port.receive();
        T data = (T) msg.readObject();
        msg.finish();
        return data;
    }

    /**
     * Returns the identifier of this port.
     * @return The port identifier.
     */
    public ReceivePortIdentifier identifier() {
        return port.identifier();
    }
}
