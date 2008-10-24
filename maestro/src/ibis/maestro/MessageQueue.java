/**
 * 
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Kees van Reeuwijk
 *
 */
class MessageQueue {

    private List<QueuedMessage> q = new LinkedList<QueuedMessage>();

    synchronized void add( IbisIdentifier destination, Message msg )
    {
        q.add( new QueuedMessage( destination, msg ) );
    }
    
    synchronized QueuedMessage getNext()
    {
        if( q.isEmpty() ){
            return null;
        }
        
        return q.remove( 0 );
    }
}
