package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;

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
    }

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
