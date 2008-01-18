package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A target in the list of outstanding ping messages.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class PingTarget {
    /** The worker we sent the ping to. */
    private final ReceivePortIdentifier worker;

    /** The time in ns we sent the ping. */
    private final long sendTime;

    /**
     * Constructs a new entry in the list of outstanding pings.
     * @param worker The worker we sent the ping to.
     * @param sendTime The time in ns we sent the ping.
     */
    PingTarget( ReceivePortIdentifier worker, long sendTime ){
	this.worker = worker;
	this.sendTime = sendTime;
    }

    ReceivePortIdentifier getWorker() { return worker; }
    long getSendTime() { return sendTime; }
}
