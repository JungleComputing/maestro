/**
 * Information that the worker maintains for a master.
 */
package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * @author Kees van Reeuwijk
 *
 */
class MasterInfo {
    /** The identifier the master wants to see when we talk to it. */
    private int identifier;

    /** The port we should send messages to. */
    final ReceivePortIdentifier port;

    MasterInfo( ReceivePortIdentifier port )
    {
	this.port = port;
	identifier = -1;   // Unknown identifier.
    }
    
    /**
     * Sets the identifier of this master.
     * @param id The identifier to set.
     */
    void setIdentifier( int id )
    {
	identifier = id;
    }
}
