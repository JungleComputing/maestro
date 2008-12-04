package ibis.maestro;

/**
 * A simple synchronized up/down counter.
 * 
 * @author Kees van Reeuwijk.
 */
class UpDownCounter {
    private int value = 0;

    UpDownCounter(int i) {
	value = i;
    }

    synchronized void up() {
	value++;
	notifyAll();
    }

    synchronized void up(int i) {
	value += i;
	notifyAll();
    }

    synchronized void down() {
	value--;
	notifyAll();
    }

    synchronized int get() {
	return value;
    }

    synchronized boolean isAbove(int v) {
	return this.value > v;
    }

    synchronized boolean isBelow(int v) {
	return this.value < v;
    }

    /**
     * Returns a string representation of this counter. (Overrides method in
     * superclass.)
     * 
     * @return The string representation.
     */
    @Override
    synchronized public String toString() {
	return Integer.toString(value);
    }

    synchronized boolean isLessOrEqual(int v) {
	return this.value <= v;
    }

    /**
     * Wait until this counter has reached a value greater or equal to the given
     * value.
     * 
     * @param n
     *            The threshold value to wait for.
     * @param duration
     *            The maximal time in ms to wait.
     * @return The actual value at the moment we stopped waiting.
     */
    public int waitForGreaterOrEqual(int n, long duration) {
	long deadline = System.currentTimeMillis() + duration;
	while (true) {
	    long waittime = deadline - System.currentTimeMillis();
	    synchronized (this) {
		if (value >= n || waittime <= 0) {
		    return value;
		}
		try {
		    this.wait(waittime);
		} catch (InterruptedException e) {
		    // Not interested.
		}
	    }
	}
    }
}
