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

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A LRU cache for ibis connections, based on <code>LinkedHashMap</code>.
 */
class SendPortCache extends LinkedHashMap<IbisIdentifier,SendPortCacheConnectionInfo>
{
    private static final long serialVersionUID = 1L;
    private static final float hashTableLoadFactor = 0.75f;
    private int useCount = 0;
    private int hits = 0;
    private int nonEssentialHits = 0;
    private int misses = 0;
    private final int cacheSize;
    private final int maximalUnusedCount;
    private int evictions = 0;

    /**
     * Creates a new LRU cache.
     * @param cacheSize the maximum number of entries that will be kept in this cache.
     * @param theMaximalUnusedCount The maximal number of cache accesses this port is
     *     not used before it is evicted.
     */
    SendPortCache( final int cacheSize, final int theMaximalUnusedCount )
    {
        super( (int) Math.ceil(cacheSize / hashTableLoadFactor) + 1, hashTableLoadFactor, true );
        this.maximalUnusedCount = theMaximalUnusedCount;
        this.cacheSize = cacheSize;
    }

    @Override
    protected boolean removeEldestEntry( Map.Entry<IbisIdentifier, SendPortCacheConnectionInfo> eldest )
    {
        SendPortCacheConnectionInfo connection = eldest.getValue();
        boolean removeIt;

        int mostRecentUse = connection.getMostRecentUse();
        synchronized( this ) {
            removeIt = (mostRecentUse+maximalUnusedCount)<useCount && size() > cacheSize;
        }
        if( removeIt ) {
            // This cache entry makes the cache too large, or has not
            // been used for too long. Out it goes.
            connection.close();
            evictions++;
            return true;
        }
        return false;
    }

    /** Given an ibis identifier, return a SendPort for that ibis.
     * The SendPort may exist already, or it may be newly created.
     * @param ibis The ibis for which we want a SendPort
     * @return The SendPort, or <code>null</code> if the ibis could not be reached.
     */
    synchronized SendPort getSendPort( IbisIdentifier ibis )
    {
        SendPortCacheConnectionInfo info;

        info = get( ibis );

        if( info == null ) {
            info = new SendPortCacheConnectionInfo();
            put( ibis, info );
            misses++;
        }
        else {
            hits++;
        }
        return info.getPort( ibis, useCount++ );
    }

    synchronized SendPort getExistingSendPort( IbisIdentifier ibis )
    {
        SendPortCacheConnectionInfo info;

        info = get( ibis );

        if( info == null ) {
            return null;
        }
        nonEssentialHits++;
        return info.getPort( ibis, useCount++ );
    }

    void closeSendPort( IbisIdentifier ibis )
    {
        SendPortCacheConnectionInfo info;
        synchronized( this ) {
            info = remove( ibis );
        }
        if( info != null ) {
            info.close();
        }
    }

    synchronized void printStatistics( PrintStream s )
    {
	s.printf( "sendport cache: %d hits, of which %d non-essential, %d misses, %d evictions\n", nonEssentialHits+hits, nonEssentialHits, misses, evictions );
    }
}
