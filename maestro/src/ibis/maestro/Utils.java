package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.Location;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
	static final long MICROSECOND_IN_NANOSECONDS = 1000L;
	static final long MILLISECOND_IN_NANOSECONDS = 1000 * MICROSECOND_IN_NANOSECONDS;
	static final long SECOND_IN_NANOSECONDS = 1000 * MILLISECOND_IN_NANOSECONDS;
	static final long MINUTE_IN_NANOSECONDS = 60 * SECOND_IN_NANOSECONDS;
	static final long HOUR_IN_NANOSECONDS = 60 * MINUTE_IN_NANOSECONDS;
	static final long DAY_IN_NANOSECONDS = 24 * HOUR_IN_NANOSECONDS;
	static final long WEEK_IN_NANOSECONDS = 7 * DAY_IN_NANOSECONDS;

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
	public static String getPlatformVersion() {
		java.util.Properties p = System.getProperties();

		return "Java " + p.getProperty("java.version") + " ("
				+ p.getProperty("java.vendor") + ") on "
				+ p.getProperty("os.name") + ' ' + p.getProperty("os.version")
				+ " (" + p.getProperty("os.arch") + ')';
	}

	/**
	 * Given an input stream, reads the entire contents of that stream into a
	 * String.
	 * 
	 * @param s
	 *            The input stream to read.
	 * @return A string containing the entire stream.
	 * @throws IOException
	 *             Thrown when there is an I/O problem.
	 */
	public static String read(InputStream s) throws IOException {
		InputStreamReader r = new InputStreamReader(s);

		StringBuffer res = new StringBuffer();
		int sz = 1000;
		char buffer[] = new char[sz];

		for (;;) {
			int n = r.read(buffer, 0, sz);
			if (n < 0) {
				break;
			}
			res.append(buffer, 0, n);
		}
		return new String(res);
	}

	/**
	 * Given a time in nanoseconds, return a neat format string for it.
	 * 
	 * @param t
	 *            The time to format.
	 * @return The formatted string.
	 */
	public static String formatNanoseconds(final long t) {
		if (t == Long.MAX_VALUE) {
			return "infinite";
		}
		if (t == 0) {
			return "0 s";
		}
		if (t < 1000L && t > -1000L) {
			return String.format("%d ns", t);
		}
		if (t < 1000000L && t > -1000000L) {
			return String.format("%4.1f us", t / 1000.0);
		}
		if (t < 1000000000L && t > -1000000000L) {
			return String.format("%4.1f ms", t / 1000000.0);
		}
		return String.format("%4.1f s", t / 1000000000.0);
	}

	/**
	 * Wait for the given thread to terminate.
	 * 
	 * @param thread
	 *            The tread to wait for.
	 */
	static void waitToTerminate(Thread thread) {
		while (thread.isAlive()) {
			try {
				thread.join();
			} catch (InterruptedException x) {
				// We don't care
			}
		}
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
	public static long divideRoundUp(long val, long divisor) {
		return (val + (divisor - 1)) / divisor;
	}

	/**
	 * Given a time in nanoseconds, return a time in milliseconds. We always
	 * round up, and make absolutely sure we don't return 0, so that it can be
	 * used as parameter for a wait method or other delay specification.
	 * 
	 * @param nanoTime
	 *            The time in nanoseconds.
	 * @return The time in milliseconds.
	 */
	public static long nanosecondsToMilliseconds(long nanoTime) {
		if (nanoTime < MILLISECOND_IN_NANOSECONDS) {
			return 1;
		}
		return divideRoundUp(nanoTime, MILLISECOND_IN_NANOSECONDS);
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

	/**
	 * Adds two longs, but return <code>Long.MAX_VALUE</code> if one of the two
	 * has that value.
	 * 
	 * @param a
	 *            One value to add.
	 * @param b
	 *            The other value to add.
	 * @return The sum of the two values, or <code>Long.MAX_VALUE</code> if one
	 *         of the inputs has that value.
	 */
	public static long safeAdd(long a, long b) {
		if (a == Long.MAX_VALUE || b == Long.MAX_VALUE) {
			return Long.MAX_VALUE;
		}
		return a + b;
	}

	/**
	 * Adds three longs, but return </code>Long.MAX_VALUE
	 * <code> if one of the three
     * has that value.
	 * 
	 * @param a
	 *            One value to add.
	 * @param b
	 *            An other value to add.
	 * @param c
	 *            A third value to add.
	 * @return The sum of the three values, or <code>Long.MAX_VALUE</code> if
	 *         one of the inputs has that value.
	 */
	public static long safeAdd(long a, long b, long c) {
		if (a == Long.MAX_VALUE || b == Long.MAX_VALUE || c == Long.MAX_VALUE) {
			return Long.MAX_VALUE;
		}
		return a + b + c;
	}

	/**
	 * Adds five longs, but return </code>Long.MAX_VALUE
	 * <code> if one of the five
     * has that value.
	 * 
	 * @param a
	 *            One value to add.
	 * @param b
	 *            An other value to add.
	 * @param c
	 *            A third value to add.
	 * @param d
	 *            A fourth value to add.
	 * @param e
	 *            A fifth value to add.
	 * @return The sum of the three values, or <code>Long.MAX_VALUE</code> if
	 *         one of the inputs has that value.
	 */
	public static long safeAdd(long a, long b, long c, long d, long e) {
		if (a == Long.MAX_VALUE || b == Long.MAX_VALUE || c == Long.MAX_VALUE
				|| d == Long.MAX_VALUE || e == Long.MAX_VALUE) {
			return Long.MAX_VALUE;
		}
		return a + b + c + d + e;
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
}
