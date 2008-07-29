package ibis.maestro;

import java.util.LinkedList;

/**
 * A message queue with synchronized add and remove methods.
 *
 * @author Kees van Reeuwijk.
 */
class MessageQueue
{
    private final LinkedList<Message> messages = new LinkedList<Message>();
    
    protected synchronized void add ( Message e )
    {
        messages.add( e );
    }

    protected synchronized Message removeIfAny()
    {
        if( messages.isEmpty() ) {
            return null;
        }
        return messages.removeFirst();
    }
   
}
