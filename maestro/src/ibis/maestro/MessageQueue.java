package ibis.maestro;

import java.io.PrintStream;
import java.util.LinkedList;

/**
 * A message queue with synchronized add and remove methods.
 *
 * @author Kees van Reeuwijk.
 */
class MessageQueue
{
    private int maxQueueSize = 0;

    private final LinkedList<Message> messages = new LinkedList<Message>();
    
    protected synchronized void add ( Message e )
    {
        messages.add( e );
        int sz = messages.size();
        if( maxQueueSize<sz ){
            maxQueueSize = sz;
        }
    }

    protected synchronized Message removeIfAny()
    {
        if( messages.isEmpty() ) {
            return null;
        }
        return messages.removeFirst();
    }


    protected synchronized void printStatistics( PrintStream s )
    {
        s.println( "Maximal message queue size: " + maxQueueSize );
    }
}
