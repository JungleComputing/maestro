package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A job request message.
 * @author Kees van Reeuwijk
 *
 */
public class JobRequest {
    private final ReceivePortIdentifier port;

    JobRequest( ReceivePortIdentifier port ){
        this.port = port;
    }
    
    ReceivePortIdentifier getPort() { return port; }
}
