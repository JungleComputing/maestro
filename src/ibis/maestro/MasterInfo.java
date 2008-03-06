/**
 * Information that the worker maintains for a master.
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.maestro.Master.WorkerIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

/**
 * @author Kees van Reeuwijk
 *
 */
class MasterInfo {
    final MasterIdentifier localIdentifier;

    /** The identifier the master wants to see when we talk to it. */
    private WorkerIdentifier identifierOnMaster;

    /** The ibis this master lives on. */
    final IbisIdentifier ibis;

    MasterInfo( MasterIdentifier localIdentifier, WorkerIdentifier identifierWithMaster, IbisIdentifier ibis )
    {
        this.localIdentifier = localIdentifier;
	this.identifierOnMaster = identifierWithMaster;
	this.ibis = ibis;
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
}
