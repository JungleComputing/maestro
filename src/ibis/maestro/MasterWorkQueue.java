package ibis.maestro;

import java.util.AbstractList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * The work queue of the master.
 * @author Kees van Reeuwijk
 *
 */
public class MasterWorkQueue {
    HashMap<JobType,AbstractList<Job>> queues = new HashMap<JobType,AbstractList<Job>>();

    void submit( Job j ) {
	JobType t = j.getType();

	AbstractList<Job> queue = queues.get( t );
	if( queue == null ) {
	    queue = new LinkedList<Job>();
	    queues.put(t, queue);
	}
	queue.add( j );

    }
}
