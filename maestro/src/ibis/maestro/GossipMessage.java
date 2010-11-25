package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A gossip message.
 * 
 * @author Kees van Reeuwijk.
 */
final class GossipMessage extends NonEssentialMessage {
    private static final long serialVersionUID = 1L;

    final NodePerformanceInfo gossip[];

    final boolean needsReply;

    GossipMessage(final IbisIdentifier target,
            final NodePerformanceInfo gossip[], final boolean needsReply) {
        super(target);
        this.gossip = gossip;
        this.needsReply = needsReply;
    }

}
