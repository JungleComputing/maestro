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
class MasterInfo {
    final MasterIdentifier localIdentifier;

    /** The identifier the master wants to see when we talk to it. */
    private WorkerIdentifier identifierOnMaster;

    private boolean dead = false;

    /** The ibis this master lives on. */
    final IbisIdentifier ibis;

    MasterInfo( MasterIdentifier localIdentifier, IbisIdentifier ibis )
    {
        this.localIdentifier = localIdentifier;
	this.identifierOnMaster = null;
	this.ibis = ibis;
    }

    boolean isRegisteredMaster()
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
