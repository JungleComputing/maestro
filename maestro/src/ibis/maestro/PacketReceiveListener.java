package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * The interface of a listener to a packet receive port.
 * 
 * @author Kees van Reeuwijk
 */
interface PacketReceiveListener {
    /**
     * Handle the reception of packet <code>packet</code>.
     * @param packet The packet that was received.
     * @param arrivalMoment The time in ns this message arrived.
     */
    void messageReceived( Message packet );

    /**
     * Returns true iff this listener is associated with the given port.
     * @param port The port it should be associated with.
     * @return True iff this listener is associated with the port.
     */
    boolean hasReceivePort( ReceivePortIdentifier port );
}
