/**
 * Maintains a list of nodes  that recently sent us work. These nodes are
 * directly (instead of through the gossip channels) kept up-to-date
 * about the state of this node.
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.ArrayList;
import java.util.List;

/**
 * @author reeuwijk
 *
 */
class RecentMasterList {
    /** The list of masters, ordered from most to least recently seen.
     * thus, element 0 is the most recently seen master.
     */
    private final List<IbisIdentifier> masters = new ArrayList<IbisIdentifier>();
    
    synchronized void register( IbisIdentifier ibis )
    {
        masters.remove( ibis );   // Remove an earlier occurence, if any.
        masters.add( 0, ibis );      // Put it at the end of the list.
        while( masters.size()>Settings.MAXIMAL_RECENT_MASTERS ){
            masters.remove( masters.size()-1 );
        }
    }
    
    /** Removes the given master from the adminstration, presumably
     * because it is gone.
     * @param ibis The ibis to remove.
     */
    synchronized void remove( IbisIdentifier ibis )
    {
        masters.remove( ibis );
    }

    synchronized IbisIdentifier[] getArray()
    {
        return masters.toArray( new IbisIdentifier[masters.size()] );
    }
}
