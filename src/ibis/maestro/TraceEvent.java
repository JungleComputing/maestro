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
	JOB,
	
	/** A job result. */
	RESULT,
	
	/** A request from work from a worker. */
	ASK_WORK,
	
	/** A ping from a master. */
	PING,
	
	/** A ping reply from a worker. */
	PING_REPLY,
	
	/** Inform a worker of new neighbors. */
	ADD_NEIGHBORS,
	
	/** Inform the master of a job result. */
	JOB_RESULT,
	
	/** Resign from this master. */
	RESIGN
    }
    
    TraceEvent( long time, ReceivePortIdentifier site, Type type, boolean sent, long id ){
	this.time = time;
	this.source = site;
	this.type = type;
	this.sent = sent;
	this.id = id;
    }
}

