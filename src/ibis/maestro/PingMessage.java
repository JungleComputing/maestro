package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message sent to a worker to test its response time.
 * 
 * @author Kees van Reeuwijk
 */
public class PingMessage extends MasterMessage {
    /** Contractual obligation */
    private static final long serialVersionUID = 1L;
    private final ReceivePortIdentifier master;
    private final double payload[];
    private static final int PAYLOAD_SIZE = 10000;
    
    /** Minimal time in ms we want to run our benchmark. */
    private static final long TARGET_INTERVAL = 100;

    /**
     * Constructs a new ping message. 
     */
    public PingMessage( ReceivePortIdentifier master )
    {
        this.master = master;
	payload = new double[PAYLOAD_SIZE];
	
	// Fill the payload with some benchmark data.
	for( int i=0; i<PAYLOAD_SIZE; i++ ) {
	    payload[i] = Math.sin( (2*i*Math.PI)/PAYLOAD_SIZE );
	}
    }
    
    ReceivePortIdentifier getMaster() { return master; }

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
	    long startTime = System.currentTimeMillis();
	    for( int iteration=0; iteration<iterations; iteration++ ) {
		for( int i=1; i<payload.length-1; i++ ) {
		    sum += Math.atan( (payload[i-1]+payload[i]+payload[i+1])/3 );
		}
	    }
	    time = System.currentTimeMillis()-startTime;
	} while( time<TARGET_INTERVAL );
	return (time/1000.0*iterations);
    }
}
