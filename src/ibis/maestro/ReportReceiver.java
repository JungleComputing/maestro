package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * 
 * A report receiver object.
 *
 * FIXME: move the communication stuff to this class.
 *
 * @author Kees van Reeuwijk.
 */
public class ReportReceiver {
    private final ReceivePortIdentifier port;
    private final long id;

    /**
     * @param port
     * @param id
     */
    public ReportReceiver(ReceivePortIdentifier port, long id) {
	this.port = port;
	this.id = id;
    }

    /**
     * 
     * @return The port to send the result to.
     */
    public ReceivePortIdentifier getPort() {
        return port;
    }

    public long getId() {
        return id;
    }

}
