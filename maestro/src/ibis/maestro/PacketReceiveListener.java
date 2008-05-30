package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;

/**
 * The interface of a listener to a packet receive port.
 * 
 * @author Kees van Reeuwijk
 * 
 * @param <T> The type of packets that are transmitted over the receive port.
 */
interface PacketReceiveListener<T extends Serializable> {
    /**
     * Handle the reception of packet <code>packet</code>.
     * @param packet The packet that was received.
     */
    void messageReceived( T packet );

    /**
     * Returns true iff this listener is associated with the given port.
     * @param port The port it should be associated with.
     * @return True iff this listener is associated with the port.
     */
    boolean hasReceivePort( ReceivePortIdentifier port );
}
