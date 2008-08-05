package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.ArrayList;

/**
 * Gossip information.
 *
 * @author Kees van Reeuwijk.
 */
class Gossip
{
    private ArrayList<NodeUpdateInfo> l = new ArrayList<NodeUpdateInfo>();

    /** FIXME.
     * @param needsReply 
     * @return
     */
    synchronized GossipMessage constructMessage( IbisIdentifier target, boolean needsReply )
    {
        NodeUpdateInfo content[] = l.toArray( new NodeUpdateInfo[l.size()] );
        return new GossipMessage( target, content, needsReply );
    }

    /** FIXME.
     * @param update
     * @return
     */
    synchronized boolean register( NodeUpdateInfo update )
    {
        for( int ix = 0; ix<l.size(); ix++ ) {
            NodeUpdateInfo i = l.get( ix );

            if( i.source.equals( update.source ) ) {
                // This is an update for the same node.
                if( update.timestamp>i.timestamp ) {
                    // This is more recent info, overwrite the old entry.
                    if( Settings.traceGossip ) {
                        Globals.log.reportProgress( "Updated gossip info about " + update.source );
                    }
                    l.set( ix, update );
                    return true;
                }
                return false;
            }
        }
        // If we reach this point, we didn't have info about this node.
        l.add( update );
        return true;
    }
    
    synchronized void removeInfoForNode( IbisIdentifier ibis )
    {
        for( int ix = 0; ix<l.size(); ix++ ) {
            NodeUpdateInfo i = l.get( ix );

            if( i.source.equals( ibis ) ) {
                l.remove( ix );
            }
        }        
    }
}
