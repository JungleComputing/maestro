package ibis.maestro;

import java.io.Serializable;

/**
 * @author Kees van Reeuwijk
 *
 * The interface of a listener to a packet receive port.
 * @param <T> The type of packets that are transmitted over the receive port.
 */
public interface PacketReceiveListener<T extends Serializable> {
    /**
     * Handle the reception of packet <code>packet</code>.
     * @param p The port the packet was received on.
     * @param packet The packet that was received.
     */
    void packetReceived( PacketUpcallReceivePort<T> p, T packet );
}
