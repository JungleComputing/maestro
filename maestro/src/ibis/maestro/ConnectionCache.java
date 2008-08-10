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
    private int hits = 0;
    private int evictions = 0;
    private final Node node;
    private final SendPortCache cache = new SendPortCache( Settings.CONNECTION_CACHE_SIZE );

    ConnectionCache( Node node )
    {
        this.node = node;
    }

    // For the moment a connection cache that doesn't cache at all.

    long cachedSendMessage( IbisIdentifier ibis, Object message )
    {
        try {
            SendPort port = cache.getSendPort( ibis );
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
    long uncachedSendMessage( IbisIdentifier ibis, Object message  )
    {
        try {
            SendPort port = Globals.localIbis.createSendPort( PacketSendPort.portType );
            port.connect( ibis, Globals.receivePortName, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT, true );
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
    long sendMessage( IbisIdentifier ibis, Object message )
    {
        long sz;

        if( Settings.CACHE_CONNECTIONS ) {
            sz = cachedSendMessage( ibis, message );
        }
        else {
            sz = uncachedSendMessage( ibis, message );
        }
        return sz;
    }

    void printStatistics( PrintStream s )
    {
        s.println( "Connection cache: " + hits + " hits, " + evictions + " evictions" );
    }

}
