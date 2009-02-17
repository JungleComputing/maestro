package ibis.maestro;

import ibis.ipl.IbisIdentifier;

abstract class NonEssentialMessage extends Message {
    private static final long serialVersionUID = 1L;

    final IbisIdentifier destination;

    NonEssentialMessage(IbisIdentifier destination) {
        this.destination = destination;
    }
}
