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
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A LRU cache for ibis connections, based on <code>LinkedHashMap</code>.
 */
public class SendPortCache
{
    private static final float hashTableLoadFactor = 0.75f;
    private int useCount = 0;
    private int hits = 0;
    private int misses = 0;

    private final class ConnectionInfo {
        private SendPort port;
        private int mostRecentUse = 0;

        synchronized SendPort getPort( IbisIdentifier ibis )
        {
            if( port == null ) {
                try {
                    port = Globals.localIbis.createSendPort( PacketSendPort.portType );
                    port.connect( ibis, Globals.receivePortName, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT, true );
                }
                catch( IOException x ) {
                    try {
                        if( port != null ) {
                            port.close();
                            port = null;
                        }
                    }
                    catch( Throwable e ) {
                        // Nothing we can do.
                    }
                }
                misses++;
            }
            else {
        	hits++;
            }
            mostRecentUse = useCount++;
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
     * @param theMaximalUnusedCount The maximal number of cache accesses this port is
     *     not used before it is evicted.
     */
    SendPortCache( final int cacheSize, final int theMaximalUnusedCount )
    {
        int hashTableCapacity = (int) Math.ceil(cacheSize / hashTableLoadFactor) + 1;
        map = new LinkedHashMap<IbisIdentifier,ConnectionInfo>( hashTableCapacity, hashTableLoadFactor, true ) {
            private final int sz = cacheSize;
            private final int maximalUnusedCount = theMaximalUnusedCount;
            private int evictions = 0;

            // (an anonymous inner class)
            private static final long serialVersionUID = 1;
            @Override
            protected boolean removeEldestEntry( Map.Entry<IbisIdentifier, ConnectionInfo> eldest ) {
                ConnectionInfo connection = eldest.getValue();
                if( (connection.mostRecentUse+maximalUnusedCount)<useCount && size() > sz ) {
                    // This cache entry makes the cache too large, or has not
                    // been used for too long. Out it goes.
		    connection.close();
		    evictions++;
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
    SendPort getSendPort( IbisIdentifier ibis )
    {
        ConnectionInfo info;

        synchronized( map ) {
            info = map.get( ibis );

            if( info == null ) {
                info = new ConnectionInfo();
                map.put( ibis, info );
            }
        }
        return info.getPort( ibis );
    }

    void closeSendPort( IbisIdentifier ibis )
    {
        ConnectionInfo info;
        synchronized( map ) {
            info = map.remove( ibis );
        }
        if( info != null ) {
            info.close();
        }
    }

    void printStatistics( PrintStream s )
    {
	// FIXME: print evictions
	s.printf( "sendport cache: %d hits, %d misses, %d evictions\n", hits, misses, 0 );
    }
}
