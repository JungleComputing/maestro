package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A message sent to a worker to tell it about new neighbor Ibises.
 * 
 * @author Kees van Reeuwijk
 */
public class AddNeighborsMessage extends MasterMessage {
    /** */
    private static final long serialVersionUID = 1L;
    private final IbisIdentifier l[];

    /**
     * @param l The list of new neighbor Ibises.
     */
    public AddNeighborsMessage(IbisIdentifier l[]) {
	this.l = l;
    }

    public IbisIdentifier[] getNeighbors() {
        return l;
    }
    
}
