package ibis.maestro;

import java.io.Serializable;

import ibis.ipl.ReceivePortIdentifier;

/**
 * 
 * A report receiver object.
 *
 * FIXME: move the communication stuff to this class.
 *
 * @author Kees van Reeuwijk.
 */
public class ReportReceiver implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 415829450459994671L;
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
     * Returns the port the report should be sent to.
     * @return The port to send the result to.
     */
    public ReceivePortIdentifier getPort() {
        return port;
    }

    /** Returns the identifier of the job this report is for.
     * 
     * @return The identifier.
     */
    public long getId() {
        return id;
    }

}
