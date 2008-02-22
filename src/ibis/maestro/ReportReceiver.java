package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

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

    public ReceivePortIdentifier getPort() {
        return port;
    }

    public long getId() {
        return id;
    }

}
