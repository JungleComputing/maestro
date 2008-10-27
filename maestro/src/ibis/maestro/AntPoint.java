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
    final IbisIdentifier ibis;
    final long timestamp;
    final int typeIndex;

    /**
     * @param ibis
     * @param timestamp
     * @param typeIndex
     */
    public AntPoint( final IbisIdentifier ibis, final long timestamp, final int typeIndex )
    {
        this.ibis = ibis;
        this.timestamp = timestamp;
        this.typeIndex = typeIndex;
    }
}
