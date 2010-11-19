package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.Location;
import ibis.steel.Estimate;

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

    private static final double SECOND = 1.0;

    static boolean areInSameCluster(final IbisIdentifier a,
            final IbisIdentifier b) {
        final Location la = a.location();
        final Location lb = b.location();
        final int nodeLevel = Math
                .min(la.numberOfLevels(), lb.numberOfLevels());
        final int matchingLevels = la.numberOfMatchingLevels(lb);
        final boolean res = matchingLevels >= nodeLevel - 1;
        return res;
    }

    /**
     * Returns a string with the platform version that is used.
     * 
     * @return The platform version.
     */
    protected static String getPlatformVersion() {
        final java.util.Properties p = System.getProperties();

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
            return String.format("%4.1f ns", 1e9 * t);
        }
        if (t < MILLISECOND && t > -MILLISECOND) {
            return String.format("%4.1f us", 1e6 * t);
        }
        if (t < SECOND && t > -SECOND) {
            return String.format("%4.1f ms", 1e3 * t);
        }
        return String.format("%4.1f s", t);
    }

    /**
     * Divide <code>val</code> by <code>divisor</code>, rounding up to the next
     * integer.
     * 
     * @param val
     *            The nominator of the division.
     * @param divisor
     *            The denominator of the division.
     * @return The result of the division.
     */
    private static long divideRoundUp(final long val, final long divisor) {
        return (val + divisor - 1) / divisor;
    }

    /**
     * Given a byte count, return a human-readable representation of it.
     * 
     * @param n
     *            The byte count to represent.
     * @return The byte count as a human-readable string.
     */
    public static String formatByteCount(final long n) {
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

    static int rankIbisIdentifiers(final IbisIdentifier local,
            final IbisIdentifier a, final IbisIdentifier b) {
        if (local == null) {
            // No way to compare if we don't know what our local ibis is.
            return 0;
        }
        final Location la = a.location();
        final Location lb = b.location();
        final Location home = local.location();
        final int na = la.numberOfMatchingLevels(home);
        final int nb = lb.numberOfMatchingLevels(home);
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
        final int hl = home.getLevel(0).hashCode();
        final int ha = la.getLevel(0).hashCode();
        final int hb = lb.getLevel(0).hashCode();
        final int da = Math.abs(hl - ha);
        final int db = Math.abs(hl - hb);
        if (da < db) {
            return -1;
        }
        if (da > db) {
            return 1;
        }
        return 0;
    }

    static void printThreadStats(final PrintStream s) {
        final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

        s.println("Peak thread count: " + threadBean.getPeakThreadCount());
        final long lockedThreads[] = threadBean.findDeadlockedThreads();
        if (lockedThreads != null && lockedThreads.length > 0) {
            s.println("===== DEADLOCKED threads =====");
            for (final long tid : lockedThreads) {
                final ThreadInfo ti = threadBean.getThreadInfo(tid,
                        Integer.MAX_VALUE);
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
        return NANOSECOND * System.nanoTime();
    }

    /**
     * Given an array of longs, return a string representation
     * 
     * @param l
     *            The array.
     * @return The string representation of the array.
     */
    public static String deepToString(final long[] l) {
        final StringBuffer buf = new StringBuffer();

        buf.append(',');
        boolean first = true;
        for (final long v : l) {
            if (first) {
                first = false;
            } else {
                buf.append(',');
            }
            buf.append(v);
        }
        return buf.toString();
    }

    static final int compareIds(final long a[], final long b[]) {
        final int sz = Math.min(a.length, b.length);
        for (int i = 0; i < sz; i++) {
            final long va = a[i];
            final long vb = b[i];

            if (va < vb) {
                return -1;
            }
            if (va > vb) {
                return 1;
            }
        }
        // At this point we know the shortest array contains
        // the same values as the longest array. Now
        // we just make the longest array larger.
        if (a.length < b.length) {
            return -1;
        }
        if (a.length > b.length) {
            return 1;
        }
        return 0;
    }

    static String formatSeconds(final Estimate t) {
        return formatSeconds(t.getAverage()) + '\u00B1'
                + formatSeconds(Math.sqrt(t.getVariance()));
    }

}
