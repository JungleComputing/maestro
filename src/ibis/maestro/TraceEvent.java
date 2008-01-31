package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;
import java.util.HashMap;

/** An event in the trace of a run. */
public class TraceEvent implements Serializable, Comparable<TraceEvent> {
    /** */
    private static final long serialVersionUID = 2246454357626562870L;
    final long time;			// When.
    final ReceivePortIdentifier source;	// From where
    final ReceivePortIdentifier dest;   // To where.
    final Type type;		// What.
    final boolean sent;		// True: sent, false: received
    final long id;

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
	ASK_WORK( "Ask work" ),
	
	/** A ping from a master. */
	PING ("Ping" ),

	/** A ping reply from a worker. */
	PING_REPLY( "Ping reply" ),
	
	/** Inform a worker of new neighbors. */
	ADD_NEIGHBORS( "Add neighbors" ),
	
	/** Inform the master of a job result. */
	JOB_RESULT( "Job result" ),
	
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
    
    TraceEvent( long time, ReceivePortIdentifier source, ReceivePortIdentifier dest, Type type, boolean sent, long id )
    {
	this.time = time;
	this.source = source;
        this.dest = dest;
	this.type = type;
	this.sent = sent;
	this.id = id;
    }

    private int getHostNumber( HashMap<ReceivePortIdentifier, Integer> sourceMap, ReceivePortIdentifier h )
    {
        if( h == null ){
            return -1;
        }
        return sourceMap.get( h );
    }
    
    public String getDescription( HashMap<ReceivePortIdentifier, Integer> sourceMap )
    {
        int srcNo = getHostNumber( sourceMap, source );
        int destNo = getHostNumber( sourceMap, dest );
        return type.getDescription() + "' P" + srcNo + "->P" + destNo + " id=" + id;
    }

    /** Print this trace event.
     * @param sourceMap The mapping between receive port and host number.
     * @param startTime The start time of the run.
     */
    public void print( long startTime, HashMap<ReceivePortIdentifier, Integer> sourceMap )
    {
        int srcNo = getHostNumber( sourceMap, source );
        int destNo = getHostNumber( sourceMap, dest );
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
    public int compareTo( TraceEvent other )
    {
        if( this.time<other.time ){
            return -1;
        }
        if( this.time>other.time ){
            return 1;
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
