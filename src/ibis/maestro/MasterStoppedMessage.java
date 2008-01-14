package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A message sent to a user to tell it that the given master has no work at the moment.
 * 
 * @author Kees van Reeuwijk
 */
public class MasterStoppedMessage extends MasterMessage {
    /** */
    private static final long serialVersionUID = 1L;
    private final IbisIdentifier master;

    /**
     * @param master The master that doesn't have work at the moment.
     */
    public MasterStoppedMessage(IbisIdentifier master) {
	this.master = master;
    }

    public IbisIdentifier getMaster() {
        return master;
    }
    
}
