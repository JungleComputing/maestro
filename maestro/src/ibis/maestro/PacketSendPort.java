package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * A port that communicates in entire objects.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class PacketSendPort {
    static final PortType portType = new PortType(
            PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT,
            PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS,
            PortType.RECEIVE_EXPLICIT);

    private final Node node; // The node this runs on.

    private final ConnectionCache connectionCache;

    private long sentBytes = 0;

    private double sendTime = 0;

    private int sentCount = 0;

    private long nonEssentialSentBytes = 0;

    private double nonEssentialSendTime = 0;

    private int nonEssentialSentCount = 0;

    private final Counter localSentCount = new Counter();

    /**
     * The list of known destinations. Register a destination before trying to
     * send to it.
     */
    private final HashMap<IbisIdentifier, DestinationInfo> destinations = new HashMap<IbisIdentifier, DestinationInfo>();

    /** One entry in the list of destinations. */
    private static final class DestinationInfo {
        private static final class InfoComparator implements
                Comparator<DestinationInfo>, Serializable {
            private static final long serialVersionUID = 9141273343902181193L;

            /**
             * Compares the two given destination info class instances. This
             * comparator ensures that the class instances are sorted by
             * decreasing sentCount. To provide a stable sort when the sentCount
             * is the same (can happen for corner cases), it also compares for
             * other fields.
             * 
             * @param a
             *            The first class instance to compare.
             * @param b
             *            The second class instance to compare.
             * @return The comparison result.
             */
            @SuppressWarnings("synthetic-access")
            @Override
            public int compare(final DestinationInfo a, final DestinationInfo b) {
                if (a.sentCount < b.sentCount) {
                    return 1;
                }
                if (a.sentCount > b.sentCount) {
                    return -1;
                }
                if (a.sentBytes < b.sentBytes) {
                    return 1;
                }
                if (a.sentBytes > b.sentBytes) {
                    return -1;
                }
                if (!a.local && b.local) {
                    return 1;
                }
                if (a.local && !b.local) {
                    return -1;
                }
                return 0;
            }

        }

        private int sentCount = 0;

        private long sentBytes = 0;

        private final IbisIdentifier ibisIdentifier;

        private final boolean local;

        /**
         * Create a new destination info entry.
         * 
         * @param ibisIdentifier
         *            The destination ibis.
         * @param local
         *            True iff this destination represents the local node.
         */
        private DestinationInfo(final IbisIdentifier ibisIdentifier,
                final boolean local) {
            this.ibisIdentifier = ibisIdentifier;
            this.local = local;
        }

        /** Print statistics for this destination. */
        private synchronized void printStatistics(final PrintStream s) {
            final char dest = local ? 'L' : 'R';
            s.format(" %c %5d messages %5s   node %s\n", dest, sentCount,
                    Utils.formatByteCount(sentBytes), ibisIdentifier.toString());
        }

        private synchronized void incrementSentCount() {
            sentCount++;
        }

        private synchronized void addSentBytes(final long val) {
            sentBytes += val;
        }
    }

    @SuppressWarnings("synthetic-access")
    PacketSendPort(final Node node, final IbisIdentifier localIbis) {
        connectionCache = new ConnectionCache(node);
        this.node = node;
        destinations.put(localIbis, new DestinationInfo(localIbis, true));
    }

    /**
     * Given a receive port, registers it with this packet send port, and
     * returns an identifier of the port.
     * 
     * @param theIbis
     *            The port to register.
     */
    @SuppressWarnings("synthetic-access")
    synchronized DestinationInfo registerDestination(
            final IbisIdentifier theIbis) {
        DestinationInfo destinationInfo = destinations.get(theIbis);
        if (destinationInfo != null) {
            // Already registered.
            return destinationInfo;
        }
        destinationInfo = new DestinationInfo(theIbis, false);
        destinations.put(theIbis, destinationInfo);
        return destinationInfo;
    }

    /**
     * Sends the given data to the given port.
     * 
     * @param theIbis
     *            The port to send it to.
     * @param message
     *            The data to send.
     * @return <code>true</code> if we managed to send the data.
     */
    @SuppressWarnings("synthetic-access")
    boolean send(final IbisIdentifier theIbis, final Message message) {
        long len;
        boolean ok = true;
        final DestinationInfo info = registerDestination(theIbis);
        info.incrementSentCount();
        if (info.local) {
            // This is the local destination. Use the back door to get
            // the info to the destination.
            message.arrivalMoment = Utils.getPreciseTime();
            message.source = theIbis;
            node.messageReceived(message);
            len = 0; // We're not going to compute a size just for the
            // statistics.
            localSentCount.add();
            if (Settings.traceSends) {
                Globals.log.reportProgress("Sent local message " + message);
            }
        } else {
            double t;

            final double startTime = Utils.getPreciseTime();
            len = connectionCache.sendMessage(theIbis, message);
            if (len < 0) {
                ok = false;
                len = 0;
            }
            synchronized (this) {
                sentBytes += len;
                sentCount++;
                t = Utils.getPreciseTime() - startTime;
                sendTime += t;
            }
            info.addSentBytes(len);
            if (Settings.traceSends) {
                Globals.log.reportProgress("Sent " + len + " bytes in "
                        + Utils.formatSeconds(t) + ": " + message);
            }
        }
        return ok;
    }

    /**
     * Given a target and a message, send a message to this target if we have an
     * open connection to it. If not, don't bother.
     * 
     * @param target
     *            The ibis to send to.
     * @param message
     *            The message to send.
     * @return True iff we managed to send the message.
     */
    @SuppressWarnings("synthetic-access")
    boolean sendNonessentialMessage(final IbisIdentifier target,
            final Message message) {
        DestinationInfo info;
        synchronized (this) {
            info = destinations.get(target);

            if (info == null) {
                return false;
            }
        }
        if (info.local) {
            // This is the local destination. Use the back door to get
            // the info to the destination.
            message.arrivalMoment = Utils.getPreciseTime();
            node.messageReceived(message);
            localSentCount.add();
            if (Settings.traceSends) {
                Globals.log.reportProgress("Sent local message " + message);
            }
            return true;
        }
        double t;

        final double startTime = Utils.getPreciseTime();
        long len = connectionCache.sendNonEssentialMessage(target, message);
        if (len < 0) {
            len = 0;
            return false;
        }
        synchronized (this) {
            nonEssentialSentBytes += len;
            nonEssentialSentCount++;
            t = Utils.getPreciseTime() - startTime;
            nonEssentialSendTime += t;
        }
        info.addSentBytes(len);
        if (Settings.traceSends) {
            Globals.log.reportProgress("Sent " + len + " bytes in "
                    + Utils.formatSeconds(t) + ": " + message);
        }
        return true;
    }

    /**
     * Given the name of this port, prints some statistics about this port.
     * 
     * @param portname
     *            The name of the port.
     */
    @SuppressWarnings("synthetic-access")
    synchronized void printStatistics(final PrintStream s, final String portname) {
        s.println(portname + ": sent " + Utils.formatByteCount(sentBytes)
                + " in " + sentCount + " remote messages; "
                + localSentCount.get() + " local sends");
        if (sentCount > 0) {
            s.println(portname + ": total send time  "
                    + Utils.formatSeconds(sendTime) + "; "
                    + Utils.formatSeconds(sendTime / sentCount)
                    + " per message");
        }
        if (nonEssentialSentCount > 0) {
            s.println(portname + ": sent "
                    + Utils.formatByteCount(nonEssentialSentBytes) + " in "
                    + nonEssentialSentCount + " remote messages");
        }
        final DestinationInfo l[] = new DestinationInfo[destinations.size()];
        int sz = 0;
        for (final Map.Entry<IbisIdentifier, DestinationInfo> entry : destinations
                .entrySet()) {
            final DestinationInfo i = entry.getValue();

            if (i != null) {
                l[sz++] = i;
            }
        }
        connectionCache.printStatistics(s);
        final Comparator<? super DestinationInfo> comparator = new DestinationInfo.InfoComparator();
        Arrays.sort(l, 0, sz, comparator);
        for (int ix = 0; ix < sz; ix++) {
            final DestinationInfo i = l[ix];

            i.printStatistics(s);
        }
    }
}
