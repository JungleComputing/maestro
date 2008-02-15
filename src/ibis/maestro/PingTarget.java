package ibis.maestro;

import java.util.ArrayList;

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

    final ArrayList<JobType> allowedTypes;

    /**
     * Constructs a new entry in the list of outstanding pings.
     * @param worker The worker we sent the ping to.
     * @param sendTime The time in ns we sent the ping.
     */
    PingTarget( ReceivePortIdentifier worker, long sendTime, ArrayList<JobType> allowedTypes ){
	this.worker = worker;
	this.sendTime = sendTime;
	this.allowedTypes = allowedTypes;
    }

    long getSendTime() { return sendTime; }

    boolean hasIdentifier(ReceivePortIdentifier id) {
        return worker.equals( id );
    }
    
    /**
     * Returns a string representation of this ping target.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "ping to " + worker + " sent at " + Service.formatNanoseconds( sendTime );
    }
}
