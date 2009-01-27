package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;

import java.io.IOException;

final class SendPortCacheConnectionInfo {
    private SendPort port;

    private int mostRecentUse = 0;

    synchronized SendPort getPort(IbisIdentifier ibis, int useCount) {
        if (port == null) {
            try {
                port = Globals.localIbis
                        .createSendPort(PacketSendPort.portType);
                port.connect(ibis, Globals.receivePortName,
                        Settings.ESSENTIAL_COMMUNICATION_TIMEOUT, true);
            } catch (IOException x) {
                try {
                    if (port != null) {
                        port.close();
                        port = null;
                    }
                } catch (Throwable e) {
                    // Nothing we can do.
                }
            }
        }
        mostRecentUse = useCount;
        return port;
    }

    synchronized void close() {
        if (port != null) {
            try {
                port.close();
            } catch (IOException e) {
                // Nothing we can do.
            }
            port = null;
        }
    }

    int getMostRecentUse() {
        return mostRecentUse;
    }
}