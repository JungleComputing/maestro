package ibis.maestro;

/**
 * 
 * An LRU cache class based on java.util.LinkedHashMap
 *
 * An LRU (least recently used) cache is used to buffer a limited number of the MRU (most recently used) objects of a class in memory.
 *
 */

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An LRU cache for ibis connections, based on <code>LinkedHashMap</code>.
 */
public class SendPortCache
{
    private static final float hashTableLoadFactor = 0.75f;

    private static final class ConnectionInfo {
        private SendPort port;
        
        synchronized SendPort getPort( IbisIdentifier ibis ) throws IOException
        {
            if( port == null ) {
                port  = Globals.localIbis.createSendPort( PacketSendPort.portType );
                port.connect( ibis, Globals.receivePortName, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT, true );
            }
            return port;
        }

        synchronized void close()
        {
            if( port != null ) {
                try{
                    port.close();
                }
                catch( IOException e ){
                    // Nothing we can do.
                }
                port = null;
            }
        }
    }

    private final HashMap<IbisIdentifier, ConnectionInfo> map;

    /**
     * Creates a new LRU cache.
     * @param cacheSize the maximum number of entries that will be kept in this cache.
     */
    public SendPortCache( final int cacheSize )
    {
        int hashTableCapacity = (int) Math.ceil(cacheSize / hashTableLoadFactor) + 1;
        map = new LinkedHashMap<IbisIdentifier,ConnectionInfo>( hashTableCapacity, hashTableLoadFactor, true ) {
            private final int sz = cacheSize;

            // (an anonymous inner class)
            private static final long serialVersionUID = 1;
            @Override protected
            boolean removeEldestEntry( Map.Entry<IbisIdentifier,ConnectionInfo> eldest ) {
                if( size() > sz ) {
                    eldest.getValue().close();
                    return true;
                }
                return false;
            }
        };
    }

    /** Given an ibis identifier, return a SendPort for that ibis.
     * The SendPort may exist already, or it may be newly created.
     * @param ibis The ibis for which we want a SendPort
     * @return The SendPort, or <code>null</code> if the ibis could not be reached.
     */
    @SuppressWarnings("synthetic-access")
    SendPort getPort( IbisIdentifier ibis )
    {
        ConnectionInfo info;

        synchronized( map ) {
            info = map.get( ibis );

            if( info == null ) {
                info = new ConnectionInfo();
                map.put( ibis, info );
            }
        }
        try{
            return info.getPort( ibis );
        }
        catch( IOException e ){
            return null;
        }
    }

}
