package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;

import java.util.LinkedList;

/**
 * The list of workers to send an accept message.
 *
 * @author Kees van Reeuwijk.
 */
class AcceptList
{
    private final LinkedList<WorkerIdentifier> list = new LinkedList<WorkerIdentifier>();

    synchronized void add( WorkerIdentifier worker )
    {
        list.add( worker );
    }
    
    synchronized WorkerIdentifier remove()
    {
        if( list.isEmpty() ) {
            return null;
        }
        return list.removeFirst();
    }
}
