package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A gossip message.
 * 
 * @author Kees van Reeuwijk.
 */
final class GossipMessage extends NonEssentialMessage {
    private static final long serialVersionUID = 1L;

    final IbisIdentifier source = Globals.localIbis.identifier();

    final NodePerformanceInfo gossip[];

    final boolean needsReply;

    GossipMessage(IbisIdentifier target, NodePerformanceInfo gossip[],
            boolean needsReply) {
        super(target);
        this.gossip = gossip;
        this.needsReply = needsReply;
    }

}
