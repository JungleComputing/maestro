package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.util.HashMap;

/** An event in the trace of a run. */
public class TransmissionEvent extends TraceEvent {
    /** */
    private static final long serialVersionUID = 2246454357626562870L;
    final ReceivePortIdentifier source;	// From where
    final ReceivePortIdentifier dest;   // To where.
    final Type type;		// What.
    final boolean sent;		// True: sent, false: received
    final long id;
    final long jobId;

    /**
     * The possible types of events.
     * 
     * @author Kees van Reeuwijk
     */
    public static enum Type {
	/** A job submission. */
	JOB( "Job" ),
	
	/** A job result. */
	RESULT ( "Result" ),
	
	/** A request from work from a worker. */
	ASK_WORK( "Ask-work" ),

	/** A ping from a master. */
	PING ("Ping" ),

	/** A ping from a master. */
	TIME_SYNC ("Time-sync" ),

	/** A ping reply from a worker. */
	PING_REPLY( "Ping-reply" ),
	
	/** Inform a worker of new neighbors. */
	ADD_NEIGHBORS( "Add-neighbors" ),
	
	/** Inform the master of a job result. */
	JOB_RESULT( "Job-result" ),
	
	/** Resign from this master. */
	RESIGN( "Resign" );

	private final String descr;

	Type( String descr ){
	    this.descr = descr;
	}
	
	String getDescription()
	{
	    return descr;
	}
    }
    
    TransmissionEvent( long time, ReceivePortIdentifier source, ReceivePortIdentifier dest, Type type, boolean sent, long id, long jobId )
    {
	super( time );
	this.source = source;
        this.dest = dest;
	this.type = type;
	this.sent = sent;
	this.id = id;
        this.jobId = jobId;
    }

    private String getHostNumber( HashMap<ReceivePortIdentifier, Integer> sourceMap, ReceivePortIdentifier h )
    {
        if( h == null ){
            return "?";
        }
        return sourceMap.get( h ).toString();
    }

    /**
     * Returns a description string for this trace event.
     * @param sourceMap The mapping of port identifiers to processor numbers.
     * @return The description string.
     */
    public String getDescription( HashMap<ReceivePortIdentifier, Integer> sourceMap )
    {
        String srcNo = getHostNumber( sourceMap, source );
        String destNo = getHostNumber( sourceMap, dest );
        return type.getDescription() + " P" + srcNo + "->P" + destNo + " id=" + id + " jobId=" + jobId;
    }

    /**
     * Returns a string representation of this trace event.
     * @return The string representation.
     */
    @SuppressWarnings("synthetic-access")
    @Override
    public String toString()
    {
	return "@" + time + (sent?" sent":" recv") + ' ' + type.descr + " id=" + id + " jobId=" + jobId;
    }

    /** Print this trace event.
     * @param sourceMap The mapping between receive port and host number.
     * @param startTime The start time of the run.
     */
    public void print( long startTime, HashMap<ReceivePortIdentifier, Integer> sourceMap )
    {
        String srcNo = getHostNumber( sourceMap, source );
        String destNo = getHostNumber( sourceMap, dest );
        if( sent ){
            System.out.println( Service.formatNanoseconds(time-startTime) + " sent '" + type.getDescription() + "' P" + srcNo + "->P" + destNo + " id=" + id );
        }
        else {
            System.out.println( Service.formatNanoseconds(time-startTime) + " received '" + type.getDescription() + "' P" + srcNo + "->P" + destNo + " id=" + id );
        }
    }

    /**
     * Compares this trace event to another.
     * We order the events on their moment of occurrence,
     * or on their id number in the unlikely event that they
     * have the same tine.
     * @param other The other event to 
     * @return The comparison result: 1: this event is larger, -1: this event is smaller, 0: both are equal.
     */
    public int compareTo( TransmissionEvent other )
    {
        if( this.time<other.time ){
            return -1;
        }
        if( this.time>other.time ){
            return 1;
        }
        if( this.sent != other.sent) {
            // Put sent events before receive events.
            return this.sent?-1:1;
        }
        if( this.id<other.id ){
            return -1;
        }
        if( this.id>other.id ){
            return 1;
        }
        return 0;
    }
}
