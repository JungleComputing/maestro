package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.LinkedList;

public class PacketUpcallReceivePort<T> implements MessageUpcall {
    private static final PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.CONNECTION_MANY_TO_ONE );
    private ReceivePort port;
    private LinkedList<PacketReceiveListener<T>> listeners = new LinkedList<PacketReceiveListener<T>>();

    /**
     * Constructs a new PacketSendPort.
     * @param ibis The ibis the port will belong to.
     * @param name The name of the port.
     * @throws IOException
     */
    PacketUpcallReceivePort( Ibis ibis, String name, PacketReceiveListener<T> listener ) throws IOException{
        port = ibis.createReceivePort(portType, name, this );
        addListener( listener );
    }

    /** Add the given listener to the list of packet reception listeners.
     * 
     * @param l The listener to add.
     */
    public void addListener( PacketReceiveListener<T> l )
    {
        synchronized( listeners ){
            listeners.add( l );
        }
    }

    /** Handle the upcall of the ipl port.
     * @param msg The message to handle.
     */
    @SuppressWarnings("unchecked")
    public void upcall(ReadMessage msg) throws IOException, ClassNotFoundException {
        T data = (T) msg.readObject();
        msg.finish();
        synchronized( listeners ){
            for( PacketReceiveListener<T> l: listeners ){
                l.packetReceived( this, data );
            }
        }
    }
    
    /**
     * Returns the identifier of this port.
     * @return The port identifier.
     */
    public ReceivePortIdentifier identifier() {
        return port.identifier();
    }
}
