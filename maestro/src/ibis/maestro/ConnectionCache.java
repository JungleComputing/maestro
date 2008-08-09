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
    private final LRUCache<IbisIdentifier,ConnectionInfo> cache = new LRUCache<IbisIdentifier,ConnectionInfo>( Settings.CONNECTION_CACHE_SIZE );
    
    private static final class ConnectionInfo {
        private SendPort port;
        
        synchronized SendPort getPort() throws IOException
        {
            if( port == null ) {
                port  = Globals.localIbis.createSendPort( PacketSendPort.portType );
            }
            return port;
        }
    }

    ConnectionCache( Node node )
    {
        this.node = node;
    }

    // For the moment a connection cache that doesn't cache at all.

    long cachedSendMessage( IbisIdentifier ibis, Object message, int timeout )
    {
        ConnectionInfo info;

        synchronized( cache ) {
            info = cache.get( ibis );
            if( info == null ) {
                info = new ConnectionInfo();
                cache.put( ibis, info );
            }
        }
        try {
            SendPort port = info.getPort();
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

    void printStatistics( PrintStream s )
    {
        // Nothing at the moment.
    }

}
