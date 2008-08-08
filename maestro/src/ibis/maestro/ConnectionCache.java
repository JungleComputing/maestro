package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Maintain a cache of open connections.
 *
 * @author Kees van Reeuwijk.
 */
public class ConnectionCache
{
    private final Node node;

    ConnectionCache( Node node )
    {
        this.node = node;
    }

    // For the moment a connection cache that doesn't cache at all.

    /**
     * Given an ibis, returns a WriteMessage to use.
     * @param ibis The ibis to send to.
     * @return The WriteMessage to fill.
     */
    long sendMessage( IbisIdentifier ibis, Object message, int timeout )
    {
        try {
            SendPort port = Globals.localIbis.createSendPort( PacketSendPort.portType );
            port.connect( ibis, Globals.receivePortName, timeout, true );
            WriteMessage msg = port.newMessage();
            msg.writeObject( message );
            return msg.finish();
        }
        catch( IOException x ){
            node.setSuspect( ibis );
            return -1;
        }
    }

    void printStatistics( PrintStream s )
    {
        // Nothing at the moment.
    }

}
