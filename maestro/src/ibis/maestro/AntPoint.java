/**
 * 
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;

/**
 * One point in an ant trail.
 * @author Kees van Reeuwijk
 *
 */
public class AntPoint implements Serializable
{
    private static final long serialVersionUID = 1L;
    final IbisIdentifier masterIbis;
    final IbisIdentifier workerIbis;
    final long timestamp;
    final int typeIndex;

    /**
     * @param masterIbis
     * @param workerIbis
     * @param timestamp
     * @param typeIndex
     */
    public AntPoint( final IbisIdentifier masterIbis, final IbisIdentifier workerIbis, final long timestamp, final int typeIndex )
    {
        this.masterIbis = masterIbis;
        this.workerIbis = workerIbis;
        this.timestamp = timestamp;
        this.typeIndex = typeIndex;
    }
}
