package ibis.maestro;

import java.io.IOException;

import ibis.ipl.Ibis;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class PacketSendPort<T> {
    private static final PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.CONNECTION_ONE_TO_ONE );
    private SendPort port;

    /**
     * Constructs a new PacketSendPort.
     * @param ibis The ibis the port will belong to.
     * @param name The name of the port.
     * @throws IOException
     */
    PacketSendPort( Ibis ibis, String name ) throws IOException{
        port = ibis.createSendPort(portType, name );
    }

    public void send( T data, ReceivePortIdentifier receiver ) throws IOException {
        port.connect(receiver);
        WriteMessage msg = port.newMessage();
        msg.writeObject( data );
        msg.finish();
        port.close();
    }
}
