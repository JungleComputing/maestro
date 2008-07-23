package ibis.maestro;

import java.util.LinkedList;

/**
 * Simple queue with locking.
 * 
 * @author Kees van Reeuwijk
 *
 * @param <T> The type of element in the list.
 */
class Queue<T> {
    private final LinkedList<T> list = new LinkedList<T>();

    protected synchronized void add (T e )
    {
        list.add( e );
    }

    protected synchronized T removeIfAny() {
        if( list.isEmpty() ) {
            return null;
        }
        return list.removeFirst();
    }

}
