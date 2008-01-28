package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;
import java.util.HashMap;

/** An event in the trace of a run. */
public class TraceEvent implements Serializable, Comparable<TraceEvent> {
    /** */
    private static final long serialVersionUID = 2246454357626562870L;
    final long time;			// When.
    final ReceivePortIdentifier source;	// Where.
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
    
    TraceEvent( long time, ReceivePortIdentifier site, Type type, boolean sent, long id )
    {
	this.time = time;
	this.source = site;
	this.type = type;
	this.sent = sent;
	this.id = id;
    }

    /** Print this trace event.
     * @param sourceMap 
     * @param startTime 
     * 
     */
    public void print(long startTime, HashMap<ReceivePortIdentifier, Integer> sourceMap)
    {
        int src = sourceMap.get( source );
        if( sent ){
            System.out.println( Service.formatNanoseconds(time-startTime) + " sent '" + type.getDescription() + "'  from P" + src + " id=" + id );
        }
        else {
            System.out.println( Service.formatNanoseconds(time-startTime) + " received '" + type.getDescription() + "'  from P" + src + " id=" + id );
        }
    }

    public int compareTo(TraceEvent other) {
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
