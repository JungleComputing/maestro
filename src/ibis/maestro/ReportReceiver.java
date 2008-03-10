package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;

/**
 * 
 * A report receiver object.
 *
 * FIXME: move the communication stuff to this class.
 *
 * @author Kees van Reeuwijk.
 */
public class ReportReceiver implements Serializable {
    private static final long serialVersionUID = 415829450459994671L;
    private final IbisIdentifier ibis;
    private final long id;

    /**
     * @param ibis The ibis to send the result to.
     * @param id The identifier to use when reporting the result.
     */
    public ReportReceiver(IbisIdentifier ibis, long id) {
	this.ibis = ibis;
	this.id = id;
    }

    /**
     * Returns the port the report should be sent to.
     * @return The port to send the result to.
     */
    public IbisIdentifier getIbis() {
        return ibis;
    }

    /** Returns the identifier of the job this report is for.
     * 
     * @return The identifier.
     */
    public long getId() {
        return id;
    }

}
