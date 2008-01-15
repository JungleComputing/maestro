package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message to tell the master not to send jobs to this worker any more.
 * 
 * @author Kees van Reeuwijk.
 */
class WorkerResignMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final ReceivePortIdentifier worker;

    ReceivePortIdentifier getPort() { return worker; }
    
    WorkerResignMessage( ReceivePortIdentifier worker ){
        this.worker = worker;
    }
}
