
package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.ArrayList;

/**
 * The ant routing table for one specific type.
 * @author Kees van Reeuwijk
 *
 */
public class AntTypeRoutingTable {
    ArrayList<NodeInfo> nodes = new ArrayList<NodeInfo>();
    long updateTimeStamp = -1;

    void addNode( NodeInfo node )
    {
        nodes.add( 0, node );
    }
    
    private int findIbis( IbisIdentifier ibis )
    {
        for( int ix=0; ix<nodes.size(); ix++ ){
            NodeInfo info = nodes.get( ix );
            
            if( info.ibis.equals(ibis ) ){
                return ix;
            }
        }
        return -1;
    }

    /** Register, using the given timestamp, that the given
     * ibis is the best route.
     * @param ibis The ibis to use.
     * @param timestamp The time the ibis was used.
     */
    synchronized void update( IbisIdentifier ibis, long timestamp )
    {
        if( timestamp>updateTimeStamp ){
            updateTimeStamp = timestamp;
            
            int ix = findIbis( ibis );
            if( ix == 0 ){
                // Already at the start, don't bother.
                return;
            }
            if( ix>0 ){
                // Put this node in front.
                NodeInfo info = nodes.remove( ix );
                nodes.add( 0, info );
            }
        }
    }
    
}
