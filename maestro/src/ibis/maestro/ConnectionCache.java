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
    private final SendPortCache cache = new SendPortCache( Settings.CONNECTION_CACHE_SIZE );

    ConnectionCache( Node node )
    {
        this.node = node;
    }

    // For the moment a connection cache that doesn't cache at all.

    long cachedSendMessage( IbisIdentifier ibis, Object message, int timeout )
    {
        try {
            SendPort port = cache.getPort( ibis );
            WriteMessage msg = port.newMessage();
            msg.writeObject( message );
            long len = msg.finish();
            return len;
        }
        catch( IOException x ){
            node.setSuspect( ibis );
            return -1;
        }
    }

    /**
     * Given an ibis, returns a WriteMessage to use.
     * @param ibis The ibis to send to.
     * @return The WriteMessage to fill.
     */
    long uncachedSendMessage( IbisIdentifier ibis, Object message, int timeout )
    {
        try {
            SendPort port = Globals.localIbis.createSendPort( PacketSendPort.portType );
            port.connect( ibis, Globals.receivePortName, timeout, true );
            WriteMessage msg = port.newMessage();
            msg.writeObject( message );
            long len = msg.finish();
            port.close();
            return len;
        }
        catch( IOException x ){
            node.setSuspect( ibis );
            return -1;
        }
    }

    /**
     * Given an ibis, returns a WriteMessage to use.
     * @param ibis The ibis to send to.
     * @return The WriteMessage to fill.
     */
    long sendMessage( IbisIdentifier ibis, Object message, int timeout )
    {
        long sz;

        if( Settings.CACHE_CONNECTIONS ) {
            sz = cachedSendMessage( ibis, message, timeout );
        }
        else {
            sz = uncachedSendMessage( ibis, message, timeout );
        }
        return sz;
    }

    void printStatistics( PrintStream s )
    {
        // Nothing at the moment.
    }

}
