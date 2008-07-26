package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * Information that the worker maintains for a master.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class NodeInfo {
    final NodeIdentifier localIdentifier;

    /** The identifier the master wants to see when we talk to it. */
    private NodeIdentifier identifierOnNode;

    /** The last time we sent this master an update. */
    private long lastUpdate = 0;
    
    private boolean suspect = false;  // This node may be dead.

    private boolean dead = false;     // This node is known to be dead.

    final boolean local;

    /** The ibis this nodes lives on. */
    final IbisIdentifier ibis;

    NodeInfo( NodeIdentifier localIdentifier, IbisIdentifier ibis, boolean local )
    {
        this.localIdentifier = localIdentifier;
	this.identifierOnNode = null;
	this.ibis = ibis;
	this.local = local;
    }

    boolean isRegistered()
    {
        return identifierOnNode != null;
    }

    /**
     * Sets the identifier that we have on this node to the given value.
     * @param workerIdentifier The identifier on this master.
     */
    void setIdentifierOnMaster( NodeIdentifier workerIdentifier )
    {
        this.identifierOnNode = workerIdentifier;
    }
    
    /**
     * Gets the identifier that we have on this node.
     * @return The identifier.
     */
    NodeIdentifier getIdentifierOnNode()
    {
        return identifierOnNode;
    }

    /**
     * @return The lastUpdate.
     */
    long getLastUpdate()
    {
        return lastUpdate;
    }

    /**
     * @param lastUpdate the lastUpdate to set
     */
    void setLastUpdate(long lastUpdate)
    {
        this.lastUpdate = lastUpdate;
    }

    /**
     * Declares this node dead.
     */
    void setDead()
    {
	suspect = true;
	dead = true;
    }

    /**
     * Returns true iff this node is dead.
     * @return Is this node dead?
     */
    boolean isDead()
    {
	return dead;
    }

    boolean isSuspect()
    {
	return suspect;
    }

    protected void setSuspect()
    {
        if( local ) {
            System.out.println( "Cannot communicate with local node " + localIdentifier + "???" );
        }
        else {
            System.out.println( "Node " + localIdentifier + " is suspect" );
            suspect = true;
        }
    }

    protected void setUnsuspect()
    {
        if( !local && suspect && !dead ) {
            System.out.println( "Node " + localIdentifier + " is no longer suspect" );
            suspect = false;
        }
    }
}
