package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.Location;

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * 
 * Global helper functions and constants.
 * 
 * @author Kees van Reeuwijk.
 */
public class Utils {
    private static final double NANOSECOND = 1e-9;
    
    static final double MICROSECOND = 1e-6;

    static final double MILLISECOND = 1e-3;

    static final double SECOND = 1.0;

    static final double MINUTE = 60 * SECOND;

    static boolean areInSameCluster(IbisIdentifier a, IbisIdentifier b) {
        Location la = a.location();
        Location lb = b.location();
        int nodeLevel = Math.min(la.numberOfLevels(), lb.numberOfLevels());
        int matchingLevels = la.numberOfMatchingLevels(lb);
        boolean res = matchingLevels >= (nodeLevel - 1);
        return res;
    }

    /**
     * Returns a string with the platform version that is used.
     * 
     * @return The platform version.
     */
    protected static String getPlatformVersion() {
        java.util.Properties p = System.getProperties();

        return "Java " + p.getProperty("java.version") + " ("
                + p.getProperty("java.vendor") + ") on "
                + p.getProperty("os.name") + ' ' + p.getProperty("os.version")
                + " (" + p.getProperty("os.arch") + ')';
    }

    /**
     * Given a time in seconds, return a neat format string for it.
     * 
     * @param t
     *            The time to format.
     * @return The formatted string.
     */
    public static String formatSeconds(final double t) {
        if (t == Double.POSITIVE_INFINITY) {
            return "infinite";
        }
        if (t == 0.0) {
            return "0 s";
        }
        if (t < MICROSECOND && t > -MICROSECOND) {
            return String.format("%4.1f ns", 1e9*t);
        }
        if (t < MILLISECOND && t > -MILLISECOND) {
            return String.format("%4.1f us", 1e6*t );
        }
        if (t < SECOND && t > -SECOND) {
            return String.format("%4.1f ms", 1e3*t );
        }
        return String.format("%4.1f s", t );
    }

    /**
     * Divide <code>val</code> by <code>divisor</code>, rounding up to the
     * next integer.
     * 
     * @param val
     *            The nominator of the division.
     * @param divisor
     *            The denominator of the division.
     * @return The result of the division.
     */
    private static long divideRoundUp(long val, long divisor) {
        return (val + (divisor - 1)) / divisor;
    }

    /**
     * Given a byte count, return a human-readable representation of it.
     * 
     * @param n
     *            The byte count to represent.
     * @return The byte count as a human-readable string.
     */
    public static String formatByteCount(long n) {
        if (n < 1000) {
            // This deliberately covers negative numbers
            return n + "B";
        }
        if (n < 1000000) {
            return divideRoundUp(n, 1000L) + "KB";
        }
        if (n < 1000000000L) {
            return divideRoundUp(n, 1000000L) + "MB";
        }
        if (n < 1000000000000L) {
            return divideRoundUp(n, 1000000000L) + "GB";
        }
        return divideRoundUp(n, 1000000000000L) + "TB";
    }

    static int rankIbisIdentifiers(IbisIdentifier local, IbisIdentifier a,
            IbisIdentifier b) {
        if (local == null) {
            // No way to compare if we don't know what our local ibis is.
            return 0;
        }
        Location la = a.location();
        Location lb = b.location();
        Location home = local.location();
        int na = la.numberOfMatchingLevels(home);
        int nb = lb.numberOfMatchingLevels(home);
        if (na > nb) {
            return -1;
        }
        if (na < nb) {
            return 1;
        }
        // Since the number of matching levels is the same, try to
        // rank on another criterium. Since we don't have access to
        // anything more meaningful, use the difference in hash values of the
        // level 0 string.
        // Although not particularly meaningful, at least the local
        // node will have distance 0, and every node will have a different
        // notion of local.
        int hl = home.getLevel(0).hashCode();
        int ha = la.getLevel(0).hashCode();
        int hb = lb.getLevel(0).hashCode();
        int da = Math.abs(hl - ha);
        int db = Math.abs(hl - hb);
        if (da < db) {
            return -1;
        }
        if (da > db) {
            return 1;
        }
        return 0;
    }

    static void printThreadStats(PrintStream s) {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

        s.println("Peak thread count: " + threadBean.getPeakThreadCount());
        long lockedThreads[] = threadBean.findDeadlockedThreads();
        if (lockedThreads != null && lockedThreads.length > 0) {
            s.println("===== DEADLOCKED threads =====");
            for (long tid : lockedThreads) {
                ThreadInfo ti = threadBean
                        .getThreadInfo(tid, Integer.MAX_VALUE);
                if (ti != null) {
                    s.println(ti.toString());
                }
            }
        }
    }

    /**
     * @return Return the precise current time in seconds.
     */
    public static double getPreciseTime() {
        return NANOSECOND*System.nanoTime();
    }
}
