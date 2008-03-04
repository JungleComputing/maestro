/**
 * Information that the worker maintains for a master.
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * @author Kees van Reeuwijk
 *
 */
class MasterInfo {
    /** The identifier of this master within our own administration. */
    final int localIdentifier;

    /** The identifier the master wants to see when we talk to it. */
    private int identifierOnMaster;

    /** The ibis this master lives on. */
    final IbisIdentifier ibis;

    MasterInfo( int localIdentifier, int identifierWithMaster, IbisIdentifier ibis )
    {
	this.localIdentifier = localIdentifier;
	this.identifierOnMaster = identifierWithMaster;
	this.ibis = ibis;
    }

    /**
     * Sets the identifier that we have on this master to the given value.
     * @param identifierOnMaster The identifier on this master.
     */
    public void setIdentifierOnMaster(int identifierOnMaster) {
        this.identifierOnMaster = identifierOnMaster;
    }
    
    /**
     * Gets the identifier that we have on this master.
     * @return The identifier.
     */
    public int getIdentifierOnMaster()
    {
        return identifierOnMaster;
    }
}
