package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Maintains a cache of open connections.
 *
 * @author Kees van Reeuwijk.
 */
class ConnectionCache
{
    private final Node node;
    private final SendPortCache cache = new SendPortCache( Settings.CONNECTION_CACHE_SIZE, Settings.CONNECTION_CACHE_MAXIMAL_UNUSED_COUNT );

    ConnectionCache( Node node )
    {
        this.node = node;
    }

    long cachedSendMessage( IbisIdentifier ibis, Object message )
    {
        long len = -1;
        try {
            SendPort port = cache.getSendPort( ibis );
            WriteMessage msg = port.newMessage();
            msg.writeObject( message );
            len = msg.finish();
        }
        catch( IOException x ){
            node.setSuspect( ibis );
            cache.closeSendPort( ibis );
        }
        return len;
    }

    /**
     * Given an ibis, returns a WriteMessage to use.
     * @param ibis The ibis to send to.
     * @return The WriteMessage to fill.
     */
    long uncachedSendMessage( IbisIdentifier ibis, Object message  )
    {
        long len = -1;
        SendPort port = null;
        try {
            port = Globals.localIbis.createSendPort( PacketSendPort.portType );
            port.connect( ibis, Globals.receivePortName, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT, true );
            WriteMessage msg = null;
            try {
                msg = port.newMessage();
                msg.writeObject( message );
            }
            finally {
                if( msg != null ) {
                    len = msg.finish();
                }
            }
            port.close();
            return len;
        }
        catch( IOException x ){
            node.setSuspect( ibis );
        }
        finally {
            try {
                if( port != null ) {
                    port.close();
                }
            }
            catch( Throwable x ) {
                // Nothing we can do.
            }
        }
        return len;
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
        cache.printStatistics( s );
    }

}
