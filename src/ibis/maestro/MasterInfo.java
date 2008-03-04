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

    public void setIdentifierOnMaster(int identifierOnMaster) {
        this.identifierOnMaster = identifierOnMaster;
    }
    
    public int getIdentifierOnMaster()
    {
        return identifierOnMaster;
    }
}
