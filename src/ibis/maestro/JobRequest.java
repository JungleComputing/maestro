package ibis.maestro;

import java.io.Serializable;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A job request message.
 * @author Kees van Reeuwijk
 *
 */
public class JobRequest implements Serializable {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final ReceivePortIdentifier port;

    JobRequest( ReceivePortIdentifier port ){
        this.port = port;
    }
    
    ReceivePortIdentifier getPort() { return port; }
}
