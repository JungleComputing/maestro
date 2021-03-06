package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Maintains a cache of open connections.
 * 
 * @author Kees van Reeuwijk.
 */
class ConnectionCache {
    private final Node node;

    private final SendPortCache cache = new SendPortCache(
            Settings.CONNECTION_CACHE_SIZE,
            Settings.CONNECTION_CACHE_MAXIMAL_UNUSED_COUNT);

    ConnectionCache(Node node) {
        this.node = node;
    }

    private long cachedSendMessage(IbisIdentifier ibis, Message message) {
        long len = -1;
        try {
            final SendPort port = cache.getSendPort(ibis);
            if (port == null) {
                // We could not create a connection to this ibis.
                Globals.log
                .reportInternalError("Could not get send port for ibis "
                        + ibis);
                node.setSuspect(ibis);
                cache.closeSendPort(ibis);
                return -1;
            }
            final WriteMessage msg = port.newMessage();
            msg.writeObject(message);
            len = msg.finish();
        } catch (final IOException x) {
            Globals.log.reportInternalError("Could not get send port for ibis "
                    + ibis + ": " + x.getLocalizedMessage() );
            final PrintStream printStream = Globals.log.getPrintStream();
            printStream.print( "------- Original stack trace: --------");
            x.printStackTrace(printStream);
            node.setSuspect(ibis);
            cache.closeSendPort(ibis);
        }
        return len;
    }

    /**
     * Given an ibis, returns a WriteMessage to use.
     * 
     * @param ibis
     *            The ibis to send to.
     * @return The WriteMessage to fill.
     */
    private long sendConnectionlessMessage(IbisIdentifier ibis, Message message) {
        long len = -1;
        SendPort port = null;
        try {
            final PortType portType = PacketSendPort.portType;
            port = Globals.localIbis.createSendPort(portType);
            port.connect(ibis, Globals.receivePortName,
                    Settings.ESSENTIAL_COMMUNICATION_TIMEOUT, true);
            WriteMessage msg = null;
            try {
                msg = port.newMessage();
                msg.writeObject(message);
            } finally {
                if (msg != null) {
                    len = msg.finish();
                }
            }
            port.close();
            return len;
        } catch (final IOException x) {
            node.setSuspect(ibis);
        } finally {
            try {
                if (port != null) {
                    port.close();
                }
            } catch (final Throwable x) {
                // Nothing we can do.
            }
        }
        return len;
    }

    /**
     * Given an ibis, returns a WriteMessage to use.
     * 
     * @param ibis
     *            The ibis to send to.
     * @return The WriteMessage to fill.
     */
    private long uncachedSendMessage(IbisIdentifier ibis, Message message) {
        long len = -1;
        SendPort port = null;
        try {
            port = Globals.localIbis.createSendPort(PacketSendPort.portType);
            port.connect(ibis, Globals.receivePortName,
                    Settings.ESSENTIAL_COMMUNICATION_TIMEOUT, true);
            WriteMessage msg = null;
            try {
                msg = port.newMessage();
                msg.writeObject(message);
            } finally {
                if (msg != null) {
                    len = msg.finish();
                }
            }
            port.close();
            return len;
        } catch (final IOException x) {
            node.setSuspect(ibis);
        } finally {
            try {
                if (port != null) {
                    port.close();
                }
            } catch (final Throwable x) {
                // Nothing we can do.
            }
        }
        return len;
    }

    /**
     * Given an ibis, returns a WriteMessage to use.
     * 
     * @param ibis
     *            The ibis to send to.
     * @return The WriteMessage to fill.
     */
    long sendMessage(IbisIdentifier ibis, Message message) {
        long sz;

        if (Settings.CACHE_CONNECTIONS) {
            sz = cachedSendMessage(ibis, message);
        } else {
            sz = uncachedSendMessage(ibis, message);
        }
        return sz;
    }

    void printStatistics(PrintStream s) {
        cache.printStatistics(s);
    }

    long sendNonEssentialMessage(IbisIdentifier ibis, Message message) {
        long len = -1;
        if (Settings.CACHE_CONNECTIONS) {
            try {
                final SendPort port = cache.getExistingSendPort(ibis);
                if (port == null) {
                    // No port in cache, don't try to send the message.
                    return -1;
                }
                final WriteMessage msg = port.newMessage();
                msg.writeObject(message);
                len = msg.finish();
            } catch (final IOException x) {
                node.setSuspect(ibis);
                cache.closeSendPort(ibis);
            }
        }
        return len;
    }

}
