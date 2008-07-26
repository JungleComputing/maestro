package ibis.maestro;

import java.io.Serializable;

/**
 * 
 * An identifier of a node.
 *
 * @author Kees van Reeuwijk.
 */
final class NodeIdentifier implements Serializable {
    private static final long serialVersionUID = 7727840589973468928L;
    private static int serialNumber = 0;  // The next serial number to hand out.

    final int value;

    private NodeIdentifier( int value )
    {
        this.value = value;
    }

    /**
     * @return A hash code for this identifier.
     */
    @Override
    public int hashCode() {
        return value;
    }

    /**
     * Compares this master identifier to the given object.
     * @param obj The object to compare to.
     * @return True iff the two identifiers are equal.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
    	return true;
        if (obj == null)
    	return false;
        if (this.getClass() != obj.getClass())
    	return false;
        final NodeIdentifier other = (NodeIdentifier) obj;
        if (value != other.value)
    	return false;
        return true;
    }


    /** Returns a string representation of this master.
     * 
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "N" + value;
    }

    /** Returns the next identifier.
     * @return The next identifier.
     */
    protected static NodeIdentifier getNextIdentifier()
    {
        return new NodeIdentifier( serialNumber++ );
    }
}