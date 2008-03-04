package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.io.Serializable;

/**
 * A Receive port for packet reception.
 * 
 * @author Kees van Reeuwijk
 *
 * @param <T> The type of the packets that will be received on this port.
 */
public class PacketUpcallReceivePort<T extends Serializable> implements MessageUpcall {
    static final PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT, PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_EXPLICIT );
    private final ReceivePort port;
    private PacketReceiveListener<T> listener;

    /**
     * Constructs a new PacketSendPort.
     * @param ibis The Ibis the port will belong to.
     * @param name The name of the port.
     * @throws IOException
     */
    PacketUpcallReceivePort( Ibis ibis, String name, PacketReceiveListener<T> listener ) throws IOException
    {
	this.listener = listener;
        port = ibis.createReceivePort(portType, name, this );
    }

    /** Handle the upcall of the ipl port. Only public because the interface requires it.
     * 
     * @param msg The message to handle.
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    @SuppressWarnings("unchecked")
    public void upcall(ReadMessage msg) throws IOException, ClassNotFoundException
    {
        T data = (T) msg.readObject();
        listener.messageReceived( this, data );
    }
    
    /**
     * Returns the identifier of this port.
     * @return The port identifier.
     */
    public ReceivePortIdentifier identifier()
    {
        return port.identifier();
    }

    /** Enable this port. */
    public void enable()
    {
	port.enableMessageUpcalls();
	port.enableConnections();
    }
}
