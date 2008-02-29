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
    final int identifier;

    /** The identifier the master wants to see when we talk to it. */
    final int identifierWithMaster;

    /** The ibis this master lives on. */
    final IbisIdentifier ibis;

    MasterInfo( int identifier, int identifierWithMaster, IbisIdentifier ibis )
    {
	this.identifier = identifier;
	this.identifierWithMaster = identifierWithMaster;
	this.ibis = ibis;
    }
}
