package ibis.maestro;

import java.io.Serializable;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A job request message.
 * @author Kees van Reeuwijk
 *
 */
public class JobRequestMessage implements Serializable {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final ReceivePortIdentifier port;

    JobRequestMessage( ReceivePortIdentifier port ){
        this.port = port;
    }
    
    ReceivePortIdentifier getPort() { return port; }
    
    /**
     * Returns a string representation of this job request.
     * @return The string.
     */
    @Override
    public String toString() {
	return "(JobRequest replyto " + port + ")"; 
    }
}
