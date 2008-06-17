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
    
    private boolean dead = false;

    /** The ibis this master lives on. */
    final IbisIdentifier ibis;

    MasterInfo( MasterIdentifier localIdentifier, IbisIdentifier ibis )
    {
        this.localIdentifier = localIdentifier;
	this.identifierOnMaster = null;
	this.ibis = ibis;
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
    long getLastUpdate() {
        return lastUpdate;
    }

    /**
     * @param lastUpdate the lastUpdate to set
     */
    void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    /**
     * Declares this master dead.
     */
    public void declareDead()
    {
	dead = true;
    }

    /**
     * Returns true iff this master is dead.
     * @return Is this master dead?
     */
    public boolean isDead()
    {
	return dead;
    }
}
