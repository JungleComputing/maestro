
package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.ArrayList;

/**
 * The ant routing table for one specific type.
 * @author Kees van Reeuwijk
 */
public class AntTypeRoutingTable {
    final TaskType type;
    final ArrayList<NodeInfo> nodes = new ArrayList<NodeInfo>();
    private long updateTimeStamp = -1;

    AntTypeRoutingTable( TaskType type )
    {
	this.type = type;
    }

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

    synchronized NodeInfo getBestReadyWorker( TaskType t )
    {
	for( int ix=0; ix<nodes.size(); ix++ ) {
	    NodeInfo node = nodes.get( ix );
	    if( node.isAvailable( t ) ) {
		return node;
	    }
	}
	return null;
    }

    synchronized void removeNode( IbisIdentifier theIbis )
    {
	int ix = findIbis( theIbis );

	if( ix>=0 ) {
	    nodes.remove( ix );
	}
    }
}
