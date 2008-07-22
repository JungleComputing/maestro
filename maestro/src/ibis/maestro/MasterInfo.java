package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.maestro.Master.WorkerIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

/**
 * Information that the worker maintains for a master.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class MasterInfo {
    final MasterIdentifier localIdentifier;

    /** The identifier the master wants to see when we talk to it. */
    private WorkerIdentifier identifierOnMaster;

    /** The last time we sent this master an update. */
    private long lastUpdate = 0;
    
    private boolean suspect = false;  // This master may be dead.

    private boolean dead = false;     // This master is known to be dead.

    final boolean local;

    /** The ibis this master lives on. */
    final IbisIdentifier ibis;

    MasterInfo( MasterIdentifier localIdentifier, IbisIdentifier ibis, boolean local )
    {
        this.localIdentifier = localIdentifier;
	this.identifierOnMaster = null;
	this.ibis = ibis;
	this.local = local;
    }

    boolean isRegistered()
    {
        return identifierOnMaster != null;
    }

    /**
     * Sets the identifier that we have on this master to the given value.
     * @param workerIdentifier The identifier on this master.
     */
    public void setIdentifierOnMaster(WorkerIdentifier workerIdentifier)
    {
        this.identifierOnMaster = workerIdentifier;
    }
    
    /**
     * Gets the identifier that we have on this master.
     * @return The identifier.
     */
    public WorkerIdentifier getIdentifierOnMaster()
    {
        return identifierOnMaster;
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
     * Declares this master dead.
     */
    void setDead()
    {
        System.out.println( "Master " + localIdentifier + " is dead" );
	suspect = true;
	dead = true;
    }

    /**
     * Returns true iff this master is dead.
     * @return Is this master dead?
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
            System.out.println( "Cannot communicate with local master " + localIdentifier + "???" );
        }
        else {
            System.out.println( "Master " + localIdentifier + " is suspect" );
            suspect = true;
        }
    }

    protected void setUnsuspect()
    {
        if( !local && suspect && !dead ) {
            System.out.println( "Master " + localIdentifier + " is no longer suspect" );
            suspect = false;
        }
    }

    /**
     * We received a message from a master, so remove any suspect label.
     * @param node  The node to notify if this is new information.
     * 
     */
    protected void setUnsuspect( Node node )
    {
        if( !local && suspect && !dead ) {
            System.out.println( "Master " + localIdentifier + " is no longer suspect" );
            suspect = false;
            node.setUnsuspectOnMaster( ibis );
        }
    }
}
