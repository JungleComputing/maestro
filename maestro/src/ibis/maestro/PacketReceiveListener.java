package ibis.maestro;

/**
 * The interface of a listener to a packet receive port.
 * 
 * @author Kees van Reeuwijk
 */
interface PacketReceiveListener {
	/**
	 * Handle the reception of packet <code>packet</code>.
	 * 
	 * @param packet
	 *            The packet that was received.
	 * @param arrivalMoment
	 *            The time in ns this message arrived.
	 */
	void messageReceived(Message packet);
}
