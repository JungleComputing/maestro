/**
 * 
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Kees van Reeuwijk
 * 
 */
class MessageQueue {

	private final ConcurrentLinkedQueue<QueuedMessage> q = new ConcurrentLinkedQueue<QueuedMessage>();

	void add(IbisIdentifier destination, Message msg) {
		q.add(new QueuedMessage(destination, msg));
	}

	QueuedMessage getNext() {
		return q.poll();
	}
}
