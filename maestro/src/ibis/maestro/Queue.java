// File: $Id: $

package ibis.maestro;

import java.util.ArrayList;

/**
 * FIXME.
 *
 * @author Kees van Reeuwijk.
 */
public class Queue
{

    protected final ArrayList<TaskInstance> queue = new ArrayList<TaskInstance>();

    /**
     * Given FIXME, constructs a new Queue.
     */
    public Queue()
    {
        super();
    }

    /**
     * Returns true iff the entire queue is empty.
     * @return
     */
    protected boolean isEmpty()
    {
        return queue.isEmpty();
    }

}