package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A message sent to a worker to tell it about new neighbor Ibises.
 * 
 * @author Kees van Reeuwijk
 */
public class AddNeighborsMessage extends MasterMessage {
    /** Contractual obligation */
    private static final long serialVersionUID = 1L;
    private final IbisIdentifier l[];

    /**
     * Constructs a new neighbor update message. 
     * @param l The list of new neighbor Ibises.
     */
    public AddNeighborsMessage(IbisIdentifier l[]) {
	this.l = l;
    }

    /**
     * Returns the list of neighbors in this message.
     * @return The list of neighbors.
     */
    public IbisIdentifier[] getNeighbors() {
        return l;
    }

    /**
     * @return The string representation of this message.
     */
    @Override
    public String toString(){
        String res = "AddNeighborsMessage[" ;
        boolean first = true;

        for( IbisIdentifier i: l ){
            if( first ){
                first = false;
            }
            else {
                res += "," + i;
            }
        }
        return res + ']';
    }
    
}
