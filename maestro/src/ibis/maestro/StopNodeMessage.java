package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A message telling the node that it should stop immediately. This is used to
 * simulate process killing to test fault-tolerance.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class StopNodeMessage extends Message {
	private static final long serialVersionUID = 5158569253342276404L;
	final IbisIdentifier source;

	/**
	 * Constructs a new stop node message.
	 */
	StopNodeMessage() {
		this.source = Globals.localIbis.identifier();
	}
}
