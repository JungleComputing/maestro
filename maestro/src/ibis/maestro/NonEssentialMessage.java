package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * Messages that are not essential for the correct functioning of the system,
 * only for efficient functioning.
 * 
 * @author Kees van Reeuwijk
 * 
 */
abstract class NonEssentialMessage extends Message {
    private static final long serialVersionUID = 1L;

    // We can make this transient because we only care about the
    // destination on the sender.
    final transient IbisIdentifier destination;

    NonEssentialMessage(final IbisIdentifier destination) {
        this.destination = destination;
    }
}
