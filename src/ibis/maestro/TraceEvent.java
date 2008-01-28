package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.Serializable;

/** An event in the trace of a run. */
public class TraceEvent implements Serializable {
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
     * 
     */
    public void print()
    {
	String action = sent?"Sent":"Received";
	System.out.println( Service.formatNanoseconds(time) + " " + action + " " + type.getDescription() + " from " + source + " id=" + id );
    }
}
