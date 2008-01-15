package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message telling a master that we would like to receive jobs.
 * @author Kees van Reeuwijk
 *
 */
public class WorkerSubscribeMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final ReceivePortIdentifier worker;

    ReceivePortIdentifier getPort() { return worker; }
    
    WorkerSubscribeMessage( ReceivePortIdentifier worker ){
        this.worker = worker;
    }
}
