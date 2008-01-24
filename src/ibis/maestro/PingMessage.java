package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message sent to a worker to test its response time.
 * 
 * @author Kees van Reeuwijk
 */
public class PingMessage extends MasterMessage {
    /** */
    private static final long serialVersionUID = -933843016931228878L;
    private final double payload[];
    private static final int PAYLOAD_SIZE = 10000;

    /** Minimal time in ms we want to run our benchmark. */
    private static final long TARGET_INTERVAL = 100;

    /**
     * Constructs a new ping message. 
     * @param master The master that sends this ping message.
     */
    public PingMessage( final ReceivePortIdentifier master )
    {
	super( master );
	payload = new double[PAYLOAD_SIZE];
	
	// Fill the payload with some benchmark data.
	for( int i=0; i<PAYLOAD_SIZE; i++ ) {
	    payload[i] = Math.sin( (2*i*Math.PI)/PAYLOAD_SIZE );
	}
    }
    
    // FIXME: remove this method.
    ReceivePortIdentifier getMaster() { return source; }

    /**
     * Run a benchmark on the payload, and return the time in
     * seconds that was required to run one iteration of that benchmark.
     * @return The list of neighbors.
     */
    public double benchmarkResult() {
	double sum = 0.0;
	int iterations = 10;
	long time;

	do {
	    iterations *= 2;
	    final long startTime = System.currentTimeMillis();
	    for( int iteration=0; iteration<iterations; iteration++ ) {
		for( int i=1; i<payload.length-1; i++ ) {
		    sum += Math.atan( (payload[i-1]+payload[i]+payload[i+1])/3 );
		}
	    }
	    time = System.currentTimeMillis()-startTime;
	} while( time<TARGET_INTERVAL );
	return (time/1000.0*iterations);
    }

    /**
     * Returns a string representation of this ping message.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "Ping message. Reply to " + source;
    }


    /**
     * Returns the event type of this message.
     */
    @Override
    protected TraceEvent.Type getMessageType()
    {
	return TraceEvent.Type.PING;
    }
}
