package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message telling a master that we would like to receive jobs.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class WorkRequestMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final ReceivePortIdentifier worker;

    ReceivePortIdentifier getPort() { return worker; }
    
    WorkRequestMessage( ReceivePortIdentifier worker ){
        this.worker = worker;
    }
}
