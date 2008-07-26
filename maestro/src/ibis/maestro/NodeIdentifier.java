// File: $Id: $

package ibis.maestro;

import java.io.Serializable;

final class NodeIdentifier implements Serializable {
    private static final long serialVersionUID = 7727840589973468928L;
    final int value;

    NodeIdentifier( int value )
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
}